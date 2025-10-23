// ------------------------------------------------------------
// Power Query Data Quality Toolkit
// (c) 2025 [Your Name or Organization]
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

let
    EndFiscalYearMonth = 0,   //set this as the last month number of your fiscal year : June = 6, July =7 etc

    StartDate= #date(2020, 1, 1),     // Change start date  #date(yyyy,m,d)   
    EndDate = DateTime.LocalNow(),  // Could change to #date(yyyy,m,d) if you need to specify future date
    YearsToFuture = 2,

    DateList = List.Dates(StartDate, Number.From(EndDate)- Number.From(StartDate) + 365 * YearsToFuture ,#duration(1,0,0,0)),

    #"Converted to Table" = Table.FromList(DateList, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Named as Date" = Table.RenameColumns(#"Converted to Table",{{"Column1", "Päivämäärä"}}),
    #"Muutettu tyyppi aluekohtaisten asetusten kanssa" = Table.TransformColumnTypes(#"Named as Date", {{"Päivämäärä", type date}}, "fi-FI"),
    #"Inserted Year" = Table.AddColumn(#"Muutettu tyyppi aluekohtaisten asetusten kanssa", "Kalenteri vuosi", each Date.Year([Päivämäärä]), type number),
    #"Inserted Month Number" = Table.AddColumn(#"Inserted Year", "Kuukauden numero", each Date.Month([Päivämäärä]), type number),
    #"Long Month Name" = Table.AddColumn(#"Inserted Month Number", "Kuukauden nimi", each Date.MonthName([Päivämäärä]), type text),
    #"Capitalized month" = Table.TransformColumns(#"Long Month Name",{{"Kuukauden nimi", Text.Proper, type text}}),
    #"Fiscal Month Number" = Table.AddColumn(#"Capitalized month", "Tilikauden kuukausi", each if [Kuukauden numero] > EndFiscalYearMonth  then [Kuukauden numero]-EndFiscalYearMonth  else [Kuukauden numero]+EndFiscalYearMonth),
    #"Changed Type1" = Table.TransformColumnTypes(#"Fiscal Month Number",{{"Tilikauden kuukausi", Int64.Type}}),
    #"Added month/year" = Table.AddColumn(#"Changed Type1", "Kalenteri kausi (kk/vvvv)", each if [Kuukauden numero] < 10 then "0" & Number.ToText([Kuukauden numero]) & "/" & Number.ToText([Kalenteri vuosi]) else Number.ToText([Kuukauden numero]) & "/" & Number.ToText([Kalenteri vuosi])),
    #"Weekday Number" = Table.AddColumn(#"Added month/year", "Viikonpäivän numero", each Date.DayOfWeek([Päivämäärä], Day.Monday)+1),
    #"ISO Week Number" = Table.AddColumn(#"Weekday Number", "Viikon numero", each if
    Number.RoundDown((Date.DayOfYear([Päivämäärä])-(Date.DayOfWeek([Päivämäärä], Day.Monday)+1)+10)/7)=0
    then
    Number.RoundDown((Date.DayOfYear(#date(Date.Year([Päivämäärä])-1,12,31))-(Date.DayOfWeek(#date(Date.Year([Päivämäärä])-1,12,31), Day.Monday)+1)+10)/7)
    else if
    (Number.RoundDown((Date.DayOfYear([Päivämäärä])-(Date.DayOfWeek([Päivämäärä], Day.Monday)+1)+10)/7)=53
    and (Date.DayOfWeek(#date(Date.Year([Päivämäärä]),12,31), Day.Monday)+1<4))
    then
    1
    else
    Number.RoundDown((Date.DayOfYear([Päivämäärä])-(Date.DayOfWeek([Päivämäärä], Day.Monday)+1)+10)/7)),
    #"Added week/year" = Table.AddColumn(#"ISO Week Number", "Kalenteri (Vko/vvvv)", each Text.Combine({Text.PadStart(Text.From([Viikon numero], "fi-FI"), 2, "0"), "-", Date.ToText([Päivämäärä], "yyyy")}), type text),
    #"Duplicated Column" = Table.DuplicateColumn(#"Added week/year", "Päivämäärä", "Viikonpäivä"),
    #"Extracted Day Name" = Table.TransformColumns(#"Duplicated Column", {{"Viikonpäivä", each Date.DayOfWeekName(_), type text}}),
    #"Capitalized day name" = Table.TransformColumns(#"Extracted Day Name",{{"Viikonpäivä", Text.Proper, type text}}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Capitalized day name",{{"Kalenteri vuosi", Int64.Type}, {"Kuukauden numero", Int64.Type}, {"Tilikauden kuukausi", Int64.Type}, {"Kalenteri kausi (kk/vvvv)", type text}, {"Viikonpäivän numero", Int64.Type}, {"Viikon numero", Int64.Type}, {"Päivämäärä", type date}}),
    #"Lajiteltu rivit" = Table.Sort(#"Changed Type2",{{"Päivämäärä", Order.Descending}}),
    #"Added mmyyyy" = Table.AddColumn(#"Lajiteltu rivit", "kkvvvv", each Number.ToText([Kuukauden numero]) & Number.ToText([Kalenteri vuosi])),
    #"Muutettu tyyppi" = Table.TransformColumnTypes(#"Added mmyyyy",{{"kkvvvv", type text}}),
    #"Added quartile number" = Table.TransformColumnTypes(Table.AddColumn(#"Muutettu tyyppi", "Vuosineljännes numero", each Date.QuarterOfYear([Päivämäärä])), {{"Vuosineljännes numero", Int64.Type}}),
    #"Added quartile" = Table.TransformColumnTypes(Table.AddColumn(#"Added quartile number", "Vuosineljännes", each "Q" & Number.ToText([Vuosineljännes numero])), {{"Vuosineljännes", type text}}),
    #"Sarakkeesta tehty kaksoiskappale" = Table.DuplicateColumn(#"Added quartile", "Kalenteri kausi (kk/vvvv)", "Kalenteri kausi (kk/vvvv) – kopio"),
    #"Jaa sarake osiin erottimen mukaan" = Table.SplitColumn(#"Sarakkeesta tehty kaksoiskappale", "Kalenteri kausi (kk/vvvv) – kopio", Splitter.SplitTextByDelimiter("/"), {"Kalenteri kausi (kk/vvvv) – kopio.1", "Kalenteri kausi (kk/vvvv) – kopio.2"}),
    #"Added yyyymm" = Table.TransformColumnTypes(Table.AddColumn(#"Jaa sarake osiin erottimen mukaan", "vvvvkk", each [#"Kalenteri kausi (kk/vvvv) – kopio.2"] & [#"Kalenteri kausi (kk/vvvv) – kopio.1"]), {{"vvvvkk", Int64.Type}}),
    #"Poistettu sarakkeet" = Table.RemoveColumns(#"Added yyyymm", {"Kalenteri kausi (kk/vvvv) – kopio.2", "Kalenteri kausi (kk/vvvv) – kopio.1"}),
    #"Added quartile/year" = Table.TransformColumnTypes(Table.AddColumn(#"Poistettu sarakkeet", "vuosineljännes_vuosi", each [Vuosineljännes] & "/" & Number.ToText([Kalenteri vuosi])), {{"vuosineljännes_vuosi", type text}}),
    #"Added vvvv/quartile number" = Table.TransformColumnTypes(Table.AddColumn(#"Added quartile/year", "vvvvq", each Number.ToText([Kalenteri vuosi]) & Number.ToText([Vuosineljännes numero])), {{"vvvvq", Int64.Type}})
in
    #"Added vvvv/quartile number"
