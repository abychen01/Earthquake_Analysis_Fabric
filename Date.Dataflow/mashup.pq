[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = List.Dates(#date(2025,1,1),1460, #duration(1,0,0,0)),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Changed column type" = Table.TransformColumnTypes(#"Converted to table", {{"Column1", type date}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Column1", "Date"}}),
  #"Inserted year" = Table.AddColumn(#"Renamed columns", "Year", each Date.Year([Date]), type nullable number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted year", {{"Year", Int64.Type}}),
  #"Inserted month" = Table.AddColumn(#"Changed column type 1", "Month", each Date.Month([Date]), type nullable number),
  #"Inserted quarter" = Table.AddColumn(#"Inserted month", "Quarter", each Date.QuarterOfYear([Date]), type nullable number),
  #"Inserted day" = Table.AddColumn(#"Inserted quarter", "Day", each Date.Day([Date]), type nullable number),
  #"Inserted day name" = Table.AddColumn(#"Inserted day", "Day name", each Date.DayOfWeekName([Date]), type nullable text),
  #"Inserted week of year" = Table.AddColumn(#"Inserted day name", "Week of year", each Date.WeekOfYear([Date]), type nullable number),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted week of year", {"Date", "Year", "Quarter", "Month", "Week of year", "Day", "Day name"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Reordered columns", {{"Quarter", Int64.Type}, {"Month", Int64.Type}, {"Week of year", Int64.Type}, {"Day", Int64.Type}})
in
  #"Changed column type 2";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "8be724d1-75c7-4a15-9b26-dc6747947d8b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2f73d050-1f8b-4e9d-bd8e-6640ac335a1c"]}[Data],
  TableNavigation = Navigation_2{[Id = "Date", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
