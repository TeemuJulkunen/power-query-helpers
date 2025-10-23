// fxDataQualityProfile: profile all columns, flag uniques, compute mean/std, detect outliers (z-score)
// Usage: fxDataQualityProfile(YourTable)
(tbl as table) as table =>
let
    ZThresh = 3.0,                              // z-score threshold for outliers
    Cols    = Table.ColumnNames(tbl),

    Profiles =
        List.Transform(Cols, (c) =>
            let
                colValues       = Table.Column(tbl, c),
                total           = List.Count(colValues),

                // Treat "" as missing for text columns
                normValues = List.Transform(colValues, each
                    if Value.Is(_, type text) and Text.Trim(_) = "" then null else _),

                nullCount       = List.Count(List.Select(normValues, each _ = null)),
                nonNull         = List.RemoveNulls(normValues),
                distinctNonNull = List.Count(List.Distinct(nonNull)),

                // Unique = no nulls AND all values distinct
                isUnique        = (nullCount = 0) and (distinctNonNull = total),

                // Data type inference (simple buckets)
                dtype =
                    if List.Count(nonNull) = 0 then "Unknown"
                    else if List.AllTrue(List.Transform(nonNull, each Value.Is(_, type number))) then "number"
                    else if List.AllTrue(List.Transform(nonNull, each Value.Is(_, type logical))) then "logical"
                    else if List.AllTrue(List.Transform(nonNull, each Value.Is(_, type date) or Value.Is(_, type datetime) or Value.Is(_, type datetimezone))) then "date/time"
                    else "text/other",

                // Numeric profiling
                nums            = List.Select(nonNull, each Value.Is(_, type number)),
                nNums           = List.Count(nums),
                mean            = if nNums > 0 then List.Average(nums) else null,
                std             = if nNums > 1 then List.StandardDeviation(nums) else null,

                // Z-score outliers (|x-mean|/std > ZThresh)
                outlierCount =
                    if std = null or std = 0 or nNums = 0 then null
                    else
                        List.Count(
                            List.Select(nums, each Number.Abs((_ - mean) / std) > ZThresh)
                        ),
                outlierPct      =
                    if outlierCount = null or nNums = 0 then null
                    else Number.Round(100.0 * outlierCount / nNums, 2)
            in
                [
                    Column           = c,
                    DataType         = dtype,
                    TotalRows        = total,
                    MissingCount     = nullCount,
                    MissingPercent   = if total = 0 then 0 else Number.Round(100.0 * nullCount / total, 2),
                    DistinctNonNull  = distinctNonNull,
                    IsUnique         = isUnique,

                    // Numeric stats (null for non-numeric columns)
                    NumericCount     = nNums,
                    Mean             = mean,
                    StdDev           = std,

                    // Outlier detection via z-score
                    ZScoreThreshold  = if nNums > 1 then ZThresh else null,
                    OutlierCount     = outlierCount,
                    OutlierPercent   = outlierPct,
                    HasOutliers      = if outlierCount = null then null else outlierCount > 0
                ]
        ),

    Result = Table.FromRecords(Profiles)
in
    Result
