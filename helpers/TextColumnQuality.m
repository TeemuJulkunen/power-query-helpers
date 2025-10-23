// fxTextColumnQuality
// Inspect all TEXT columns for quality issues with robust whitespace handling
// Usage: fxTextColumnQuality(MyTable)
(tbl as table) as table =>
let
    Cols = Table.ColumnNames(tbl),

    // Define a robust whitespace set: space, tab, CR, LF, non-breaking space
    WhiteSet = {
        " ",
        Character.FromNumber(9),   // tab
        Character.FromNumber(10),  // LF
        Character.FromNumber(13),  // CR
        Character.FromNumber(160)  // NBSP
    },

    IsTextCol = (values as list) as logical =>
        let nn = List.RemoveNulls(values)
        in if List.Count(nn)=0 then false else List.AllTrue(List.Transform(nn, each Value.Is(_, type text))),

    HasNonPrintable = (t as text) as logical =>
        let
            chars = Text.ToList(t),
            codes = List.Transform(chars, each Character.ToNumber(_)),
            bad = List.AnyTrue(List.Transform(codes, each _ < 32 or _ = 127))
        in
            bad,

    ProfileOne =
        List.Transform(Cols, (c) =>
            let
                vals = Table.Column(tbl, c),
                isText = IsTextCol(vals)
            in
                if not isText then null else
                let
                    total   = List.Count(vals),
                    nonNull = List.RemoveNulls(vals),
                    nulls   = total - List.Count(nonNull),

                    // Leading / trailing detection using TrimStart/TrimEnd with explicit whitespace set
                    leadFlags = List.Transform(nonNull, each
                        let s = _ in Text.Length(s) > Text.Length(Text.TrimStart(s, WhiteSet))
                    ),
                    trailFlags = List.Transform(nonNull, each
                        let s = _ in Text.Length(s) > Text.Length(Text.TrimEnd(s, WhiteSet))
                    ),

                    leadingCount      = List.Count(List.Select(leadFlags, each _ = true)),
                    trailingCount     = List.Count(List.Select(trailFlags, each _ = true)),
                    bothSidesCount    =
                        List.Count(
                            List.Select(
                                List.Zip({leadFlags, trailFlags}),
                                each _{0} = true and _{1} = true
                            )
                        ),
                    leadingOnlyCount  =
                        List.Count(
                            List.Select(
                                List.Zip({leadFlags, trailFlags}),
                                each _{0} = true and _{1} = false
                            )
                        ),
                    trailingOnlyCount =
                        List.Count(
                            List.Select(
                                List.Zip({leadFlags, trailFlags}),
                                each _{0} = false and _{1} = true
                            )
                        ),
                    anyLeadOrTrail    = leadingCount + trailingCount - bothSidesCount,

                    // Whitespace-only (after full trim becomes empty)
                    whitespaceOnly = List.Count(List.Select(nonNull, each Text.Length(Text.Trim(_, WhiteSet)) = 0)),

                    // Non-printable ASCII check
                    nonPrintable = List.Count(List.Select(nonNull, each HasNonPrintable(_))),

                    // Length stats
                    lengths = List.Transform(nonNull, each Text.Length(_)),
                    avgLen = if List.Count(lengths)=0 then null else Number.Round(List.Average(lengths), 2),
                    minLen = if List.Count(lengths)=0 then null else List.Min(lengths),
                    maxLen = if List.Count(lengths)=0 then null else List.Max(lengths),

                    // Distincts
                    distinctCS = List.Count(List.Distinct(nonNull)),
                    distinctCI = List.Count(List.Distinct(List.Transform(nonNull, each Text.Lower(_))))
                in
                    [
                        Column = c,
                        TotalRows = total,
                        NullCount = nulls,

                        // Leading & trailing diagnostics
                        LeadingSpaces        = leadingCount,
                        TrailingSpaces       = trailingCount,
                        LeadingOnly          = leadingOnlyCount,
                        TrailingOnly         = trailingOnlyCount,
                        BothLeadingAndTrailing = bothSidesCount,
                        AnyLeadingOrTrailing = anyLeadOrTrail,

                        // Other text quality
                        WhitespaceOnly       = whitespaceOnly,
                        NonPrintableChars    = nonPrintable,

                        // Length & distincts
                        AvgLength = avgLen,
                        MinLength = minLen,
                        MaxLength = maxLen,
                        Distinct_CaseSensitive = distinctCS,
                        Distinct_CaseInsensitive = distinctCI
                    ]
        ),

    Kept = List.RemoveNulls(ProfileOne),
    Result = Table.FromRecords(Kept)
in
    Result
