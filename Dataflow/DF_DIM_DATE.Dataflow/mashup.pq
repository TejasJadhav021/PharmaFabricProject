[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared dim_date = let
    StartDate = #date(2020,1,1),
    EndDate = #date(2035,12,31),

    NumberOfDays = Duration.Days(EndDate-StartDate)+1,

    Dates = List.Dates(
        StartDate,
        NumberOfDays,
        #duration(1,0,0,0)
    ),

    TableFromDates = Table.FromList(
        Dates,
        Splitter.SplitByNothing(),
        {"FULL_DATE"}
    ),

    ChangedType =
        Table.TransformColumnTypes(
            TableFromDates,
            {{"FULL_DATE", type date}}
        ),
  #"Inserted year" = Table.AddColumn(ChangedType, "Year", each Date.Year([FULL_DATE]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted year", "Month", each Date.Month([FULL_DATE]), type nullable number),
  #"Inserted month name" = Table.AddColumn(#"Inserted month", "Month name", each Date.MonthName([FULL_DATE]), type nullable text),
  #"Inserted quarter" = Table.AddColumn(#"Inserted month name", "Quarter", each Date.QuarterOfYear([FULL_DATE]), type nullable number),
  #"Inserted week of year" = Table.AddColumn(#"Inserted quarter", "Week of year", each Date.WeekOfYear([FULL_DATE]), type nullable number),
  #"Inserted day" = Table.AddColumn(#"Inserted week of year", "Day", each Date.Day([FULL_DATE]), type nullable number),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Inserted day", "DATE_KEY", each Date.Year([FULL_DATE])*10000 +
Date.Month([FULL_DATE])*100 +
Date.Day([FULL_DATE])), {{"DATE_KEY", Int64.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "YEAR_MONTH", each Text.From(Date.Year([FULL_DATE])) &
"-" &
Text.PadStart(Text.From(Date.Month([FULL_DATE])),2,"0")),
  #"Inserted day name" = Table.AddColumn(#"Added custom 1", "Day name", each Date.DayOfWeekName([FULL_DATE]), type nullable text),
  #"Renamed columns" = Table.RenameColumns(#"Inserted day name", {{"Day name", "Day_name"}})

in
    #"Renamed columns";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "16650313-386a-4ca3-8c9e-ff3ca2b04f6d"]}[Data]{[lakehouseId = "7f3feb17-1239-4f7a-b910-f14143f53674"]}[Data];
