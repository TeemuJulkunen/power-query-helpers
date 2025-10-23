// ------------------------------------------------------------
// Power Query Data Quality Toolkit
// (c) 2025 Teemu Julkunen
//
// Licensed under the MIT License:
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// ------------------------------------------------------------

// fxCategoricalStats
// Summarize all TEXT columns: distincts, nulls, and Top-N values
// Usage: fxCategoricalStats(MyTable, 5, true)
(tbl as table, optional topN as nullable number, optional includeNulls as nullable logical) as table =>
let
    N = if topN = null then 5 else topN,
    IncNulls = if includeNulls = null then false else includeNulls,

    Cols = Table.ColumnNames(tbl),

    IsTextCol = (values as list) as logical =>
        let
            nn = List.RemoveNulls(values),
            any = if List.Count(nn)=0 then false else true,
            allText = if any then List.AllTrue(List.Transform(nn, each Value.Is(_, type text))) else false
        in
            allText,

    BuildTopN = (values as list, n as number, incNulls as logical) as table =>
        let
            asText = List.Transform(values, each if _ = null then null else Text.From(_)),
            base = if incNulls then asText else List.RemoveNulls(asText),
            cnt = List.Count(base),
            grp = Table.FromColumns({base}, {"Value"}),
            grp2 = Table.Group(grp, {"Value"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
            sorted = Table.Sort(grp2, {{"Count", Order.Descending}, {"Value", Order.Ascending}}),
            top = Table.FirstN(sorted, n),
            withPct = if cnt=0 then top else Table.AddColumn(top, "Percent", each Number.Round(100.0 * [Count]/cnt, 2))
        in
            withPct,

    ProfilePerCol =
        List.Transform(Cols, (c) =>
            let
                colValues = Table.Column(tbl, c),
                isText = IsTextCol(colValues)
            in
                if not isText then
                    null
                else
                    let
                        total = List.Count(colValues),
                        nulls = List.Count(List.Select(colValues, each _ = null)),
                        nonNulls = total - nulls,
                        distinctNonNull = List.Count(List.Distinct(List.RemoveNulls(colValues))),
                        top = BuildTopN(colValues, N, IncNulls)
                    in
                        [
                            Column = c,
                            TotalRows = total,
                            NullCount = nulls,
                            NonNullCount = nonNulls,
                            DistinctNonNull = distinctNonNull,
                            TopValues = top
                        ]
        ),

    Kept = List.RemoveNulls(ProfilePerCol),
    Result = Table.FromRecords(Kept)
in
    Result
