#region Using directives

using FTOptix.Alarm;
using FTOptix.Core;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;

#endregion

public class ImportAndExportAlarms : BaseNetLogic
{
    private static readonly List<string> commonProperties = new List<string>() { "Enabled", "AutoAcknowledge", "AutoConfirm", "Severity", "Message", "LocalizedMessage", "MessageHighHighState", "MessageHighState", "MessageLowState", "MessageLowLowState", "LocalizedMessageHighHighState", "LocalizedMessageHighState", "LocalizedMessageLowState", "LocalizedMessageLowLowState", "HighHighLimit", "HighLimit", "LowLowLimit", "LowLimit", "LastEvent", "InputValue", "NormalStateValue", "Setpoint", "PollingTime", "MaxTimeShelved", "PresetTimeShelved", "LatchingEnabled" };

    [ExportMethod]
    public void ImportAlarms()
    {
        var folderPath = GetCSVFilePath();
        if (string.IsNullOrEmpty(folderPath))
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "No CSV file chosen, please fill the CSVPath variable");
            return;
        }

        char fieldDelimiter = (char)GetFieldDelimiter();
        if (fieldDelimiter == '\0')
            return;

        bool wrapFields = GetWrapFields();

        foreach (string file in Directory.EnumerateFiles(folderPath, "*.csv"))
        {
            try
            {
                using (CsvFileReader reader = new(file) { FieldDelimiter = fieldDelimiter, WrapFields = wrapFields })
                {
                    List<CsvUaObject> csvUaObjects = new();
                    List<string> headerColumns = reader.ReadLine();
                    while (!reader.EndOfFile())
                    {
                        CsvUaObject obj = GetDataFromCsvRow(reader.ReadLine(), headerColumns);
                        // if no data is read from csv, exit from while
                        if (obj == null)
                            continue;
                        csvUaObjects.Add(obj);
                    }
                    if (csvUaObjects.Count == 0)
                    {
                        Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"CSV file '{file}' does not contain valid objects to be imported");
                        continue;
                    }
                    List<string> objectTypesIntoFile = csvUaObjects.Select(o => o.TypeBrowsePath).Distinct().ToList();
                    if (objectTypesIntoFile.Count == 0)
                    {
                        Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"CSV file '{file}' contains no data for object type: {string.Join(",", objectTypesIntoFile)}. CSV file '{file}' will be skipped!");
                        continue;
                    }
                    else if (objectTypesIntoFile.Count > 1)
                    {
                        Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"CSV file '{file}' contains data of more than one object type, found: '{string.Join(", ", objectTypesIntoFile)}'. CSV file '{file}' will be skipped!");
                        continue;
                    }
                    // Get the name of Alarm type by removing all path
                    string csvObjectsCommonType = objectTypesIntoFile.FirstOrDefault();

                    NodeId nativeAlarmTypeNodeID = null;
                    IUANode myNewAlarmType = Project.Current.Find(csvObjectsCommonType);
                    // in case of the type not exist in the project, try to check if is a common FTOptix Alarm type
                    if (myNewAlarmType == null)
                    {
                        // Check if the current alarm type is native
                        nativeAlarmTypeNodeID = (NodeId)typeof(FTOptix.Alarm.ObjectTypes).GetFields().FirstOrDefault(x => x.Name.Equals(csvObjectsCommonType, StringComparison.InvariantCultureIgnoreCase) && x.FieldType == typeof(NodeId), null)?.GetValue(null);
                        if (nativeAlarmTypeNodeID != null)
                        {
                            // Get the node of the native Alarm type
                            myNewAlarmType = InformationModel.Get(nativeAlarmTypeNodeID);
                        }
                        else
                        {
                            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Object Type {csvObjectsCommonType} does not exist in the current FTOptix project. CSV file '{file}' will be skipped!");
                            continue;
                        }
                    }

                    // make sure the custom type described by the CSV is an Alarm
                    if (!myNewAlarmType.GetType().IsAssignableTo(typeof(AlarmControllerType)))
                    {
                        Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Object Type '{csvObjectsCommonType}' is not an an alarm subtype, CSV file '{file}' will be skipped!");
                        continue;
                    }

                    // Loop per each alarm in the CSV file
                    foreach (CsvUaObject csvUaObject in csvUaObjects)
                    {
                        _ = CreateFoldersTreeFromPath(csvUaObject.BrowsePath);
                        Project.Current.Get(csvUaObject.BrowsePath).Children.Remove(csvUaObject.Name);
                        IUAObject myNewAlarm = InformationModel.MakeObject(csvUaObject.Name, myNewAlarmType.NodeId);
                        //Check all common properties and set their values from CSV
                        foreach (string commonProperty in commonProperties)
                        {
                            string commonPropertyValue = csvUaObject.Variables.SingleOrDefault(v => v.Key == commonProperty).Value;
                            if (!string.IsNullOrEmpty(commonPropertyValue))
                            {
                                if (commonProperty.Contains("Message", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    IUAVariable propertyVariable = null;
                                    if (!commonProperty.Contains("Localized"))
                                    {
                                        if (myNewAlarm is LimitAlarmController myNewlimitAlarmController)
                                        {
                                            if (commonProperty.Contains("HighHigh"))
                                            {
                                                propertyVariable = myNewlimitAlarmController.MessageHighHighStateVariable;
                                            }
                                            else if (commonProperty.Contains("LowLow"))
                                            {
                                                propertyVariable = myNewlimitAlarmController.MessageLowLowStateVariable;
                                            }
                                            else if (commonProperty.Contains("High"))
                                            {
                                                propertyVariable = myNewlimitAlarmController.MessageHighStateVariable;
                                            }
                                            else if (commonProperty.Contains("Low"))
                                            {
                                                propertyVariable = myNewlimitAlarmController.MessageLowStateVariable;
                                            }
                                        }
                                        if (commonProperty.Equals("Message", StringComparison.InvariantCultureIgnoreCase))
                                        {
                                            propertyVariable = myNewAlarm.GetOrCreateVariable(commonProperty);
                                        }
                                    }
                                    if (!SetValueProperty(propertyVariable, commonProperty, commonPropertyValue, myNewAlarm.BrowseName))
                                    {
                                        // Set the message or translation key for this alarm
                                        SetAlarmMessage((AlarmController)myNewAlarm, commonProperty, commonPropertyValue);
                                    }
                                }
                                else if (!SetValueProperty(myNewAlarm.GetOrCreateVariable(commonProperty), commonProperty, commonPropertyValue, myNewAlarm.BrowseName))
                                {
                                    if (!IsNotOnlyNumberOrBooleanOrDurationValue().IsMatch(commonPropertyValue))
                                    {
                                        Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Unable to find variable {commonPropertyValue} for the alarm property {commonProperty} in the alarm {myNewAlarm.BrowseName}, value set as normal and not Dynamic Link");
                                    }
                                    // Try setting the value read in the csv file
                                    TrySetOptimalProperty((AlarmController)myNewAlarm, commonProperty, commonPropertyValue);
                                }
                            }
                        }
                        // Get all uncommon properties of the Alarm to set its the value
                        foreach (var (property, customPropertyValue) in
                                 from property in myNewAlarm.Children.Where(x => !commonProperties.Contains(x.BrowseName))
                                 let valueProperty = csvUaObject.Variables.SingleOrDefault(v => v.Key == property.BrowseName).Value
                                 select (property, valueProperty))
                        {
                            if (!SetValueProperty(myNewAlarm.GetVariable(property.BrowseName), property.BrowseName, customPropertyValue, myNewAlarm.BrowseName))
                            {
                                TrySetOptimalProperty((AlarmController)myNewAlarm, property.BrowseName, customPropertyValue);
                            }
                        }
                        Project.Current.Get(csvUaObject.BrowsePath).Children.Add(myNewAlarm);
                    }
                }
                Log.Info("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Alarms successfully imported from " + file);
            }
            catch (Exception ex)
            {
                Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Unable to import alarms from {file}, error message: {ex.Message}");
            }
        }
    }

    private static void SetAlarmMessage(AlarmController myNewAlarm, string commonProperty, string propertyValue)
    {
        // If the property is related to messages (both key and content)
        foreach (var alarmProperty in myNewAlarm.GetType().GetProperties())
        {
            if (alarmProperty.Name == commonProperty)
            {
                object valueToSet = null;
                if (alarmProperty.Name.Contains("Localized"))
                {
                    string messageKey = propertyValue;
                    if (!string.IsNullOrEmpty(messageKey))
                    {
                        LocalizedText alarmKey = new LocalizedText(myNewAlarm.NodeId.NamespaceIndex, messageKey);
                        if (string.IsNullOrEmpty(InformationModel.LookupTranslation(alarmKey).Text))
                        {
                            Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Unable to find translation for the key {messageKey} in the Alarm {myNewAlarm.BrowseName}");
                            return;
                        }
                        myNewAlarm.Message = "";
                        valueToSet = new LocalizedText(myNewAlarm.NodeId.NamespaceIndex, messageKey);
                    }
                }
                else
                {
                    string message = propertyValue;
                    // If this alarm already has a translation, we cannot set the plain message
                    LocalizedText alarmMessageKey = (LocalizedText)myNewAlarm.GetType().GetProperty("Localized" + commonProperty).GetValue(myNewAlarm, null);
                    if (alarmMessageKey?.HasTextId == false && !string.IsNullOrEmpty(message))
                        valueToSet = message;
                }
                if (valueToSet != null)
                    alarmProperty.SetValue(myNewAlarm, valueToSet);
            }
        }
    }

    private static void TrySetOptimalProperty(AlarmController alarm, string propertyName, string propertyValue)
    {
        // If property is null or empty, exit from void
        if (propertyValue?.Length == 0 || propertyValue == null)
            return;
        UAValue defaultPropertyValue;
        try
        {
            defaultPropertyValue = alarm.GetOptionalVariableValue(propertyName);
        }
        catch
        {
            defaultPropertyValue = null;
        }
        if (defaultPropertyValue != null)
        {
            // Switch between the type of the property value
            switch (defaultPropertyValue.Value)
            {
                case bool:
                    alarm.SetOptionalVariableValue(propertyName, ConvertStringToBool(propertyValue));
                    break;

                case int:
                    if (!int.TryParse(propertyValue, out int intValue))
                        _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                    else
                        alarm.SetOptionalVariableValue(propertyName, intValue);
                    break;

                case double:
                    var defaultProperty = alarm.GetOrCreateVariable(propertyName);
                    if (defaultProperty.DataType == OpcUa.DataTypes.Duration && TimeSpan.TryParse(propertyValue, CultureInfo.InvariantCulture.DateTimeFormat, out TimeSpan durationValue))
                    {
                        alarm.SetOptionalVariableValue(propertyName, durationValue.TotalMilliseconds);
                    }
                    else
                    {
                        if (!double.TryParse(propertyValue, out double doubleValue))
                        {
                            _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                        }
                        else
                        {
                            alarm.SetOptionalVariableValue(propertyName, doubleValue);
                        }
                    }
                    break;

                case float:
                    if (!float.TryParse(propertyValue, out float floatValue))
                        _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                    else
                        alarm.SetOptionalVariableValue(propertyName, floatValue);
                    break;

                case ushort:
                    if (!ushort.TryParse(propertyValue, out ushort ushortValue))
                        _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                    else
                        alarm.SetOptionalVariableValue(propertyName, ushortValue);
                    break;

                case uint:
                    if (!uint.TryParse(propertyValue, out uint uintValue))
                        _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                    else
                        alarm.SetOptionalVariableValue(propertyName, uintValue);
                    break;

                case ulong:
                    if (!ulong.TryParse(propertyValue, out ulong ulongValue))
                        _ = SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName);
                    else
                        alarm.SetOptionalVariableValue(propertyName, ulongValue);
                    break;

                case LocalizedText:
                    var alarmKey = new LocalizedText(alarm.NodeId.NamespaceIndex, propertyValue);
                    UAValue valueToSet;
                    if (string.IsNullOrEmpty(InformationModel.LookupTranslation(alarmKey).Text))
                    {
                        Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Unable to find translation for the key {propertyValue} in the Alarm property {propertyName} in {alarm.BrowseName}");
                        valueToSet = new(propertyValue);
                    }
                    else
                    {
                        valueToSet = new(new LocalizedText(alarm.NodeId.NamespaceIndex, propertyValue));
                    }
                    alarm.SetOptionalVariableValue(propertyName, valueToSet);
                    break;

                default:
                    // in case of is an unknown type, try to manage as dynamic link, otherwise set the value
                    if (!SetValueProperty(alarm.GetOrCreateVariable(propertyName), propertyName, propertyValue, alarm.BrowseName))
                    {
                        alarm.SetOptionalVariableValue(propertyName, propertyValue);
                    }
                    break;
            }
        }
    }

    private static bool ConvertStringToBool(string value)
    {
        return value == "1" || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    private static bool SetValueProperty(IUAVariable alarmField, string propertyBrowseName, string propertyValue, string alarmName)
    {
        if (alarmField == null || string.IsNullOrEmpty(propertyValue))
            return false;
        IUAVariable dynamicLinkVariable;
        // First check if it is an Alias or a Variable. For Alias check if the string start with a value in angle brackets
        if (Regex.IsMatch(propertyValue, @"^\{.*?\}"))
        {
            // Create a Variable of type DynamicLink with datatype NodePath
            DynamicLink aliasDynamicLinkVar = InformationModel.MakeVariable<DynamicLink>("AppCrAl_" + propertyBrowseName, FTOptix.Core.DataTypes.NodePath);
            // Set value with the full Alias path {AliasName}/Path1/Path2/../Var
            aliasDynamicLinkVar.Value = propertyValue;
            // Set reference to Alarm field with the DynamicLink
            alarmField.Refs.AddReference(FTOptix.CoreBase.ReferenceTypes.HasDynamicLink, aliasDynamicLinkVar);
        }
        else
        {
            // If the string in the csv is an array, I extract the value. Regular expression check square brackets with digit values
            string findBracketsRegex = @"\[.*?\d\]";
            // If the string in the csv is a variable with bit notation (var.<bit position>), I extract the value. Regular check the presence of numbers after the dot.
            string findIndexedWords = @"\.\d{1,2}\z";
            if (Regex.IsMatch(propertyValue, findBracketsRegex) || Regex.IsMatch(propertyValue, findIndexedWords))
            {
                Regex valueToExtract;
                // get the variable name without brackets or bit notation
                string targetVar = Regex.Replace(propertyValue, findBracketsRegex, "");
                targetVar = Regex.Replace(targetVar, findIndexedWords, "");
                // Variable name is the full key-value without the match of regex. try to get from the project
                dynamicLinkVariable = Project.Current.GetVariable(targetVar);
                // If the result of GetVariable is null, write a warning and return
                if (dynamicLinkVariable == null)
                {
                    Log.Warning("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Unable to find the variable {targetVar} for the alarm property {propertyBrowseName} in the alarm {alarmName}");
                    return true;
                }
                // First set the variable (without brackets or bit notation)
                alarmField.SetDynamicLink(dynamicLinkVariable);
                // in case of Array set the variable name with the brackets and the index
                if (Regex.IsMatch(propertyValue, findBracketsRegex))
                {
                    // Replace the value of dynamic link with variable name and the brackets (the regex match value)
                    valueToExtract = new Regex(findBracketsRegex);
                    alarmField.GetVariable("DynamicLink").Value += valueToExtract.Match(propertyValue).Value;
                }
                // In case of bit notation (also applicable to arrays, that is why the check occurs after the first one), set the variable name in dynamic link with bit notation
                if (Regex.IsMatch(propertyValue, findIndexedWords))
                {
                    // Replace the value of dynamic link with variable name and the bit notation
                    valueToExtract = new Regex(findIndexedWords);
                    alarmField.GetVariable("DynamicLink").Value += valueToExtract.Match(propertyValue).Value;
                }
            }
            else
            {
                // Try to get the variable from the project
                dynamicLinkVariable = Project.Current.GetVariable(propertyValue);
                // If the result of GetVariable is null, write a warning and return
                if (dynamicLinkVariable == null)
                {
                    return false;
                }
                // Create the dynamic link
                alarmField.SetDynamicLink(dynamicLinkVariable);
            }
        }
        return true;
    }

    private List<IUAObjectType> GetAlarmTypeList()
    {
        List<NodeId> filteredTypes = [FTOptix.Alarm.ObjectTypes.LimitAlarmController, FTOptix.Alarm.ObjectTypes.ExclusiveLimitAlarmController, FTOptix.Alarm.ObjectTypes.NonExclusiveLimitAlarmController];
        var alarms = new List<IUAObjectType>();
        var projectNamespaceIndex = LogicObject.NodeId.NamespaceIndex;
        // Insert code to be executed by the method
        var alarmControllerType = InformationModel.Get(FTOptix.Alarm.ObjectTypes.AlarmController);
        var allControllerTypes = new List<IUAObjectType>();
        CollectRecursive((IUAObjectType)alarmControllerType, allControllerTypes);
        var concreteTypes = allControllerTypes.Where(x => !filteredTypes.Contains(x.NodeId)).ToList().FindAll(type => !type.IsAbstract);
        alarms.AddRange(concreteTypes);
        var userDefinedTypes = concreteTypes.FindAll(type => type.NodeId.NamespaceIndex == projectNamespaceIndex);
        alarms.AddRange(userDefinedTypes);
        return alarms;
    }

    [ExportMethod]
    public void ExportAlarms()
    {
        var csvDir = GetCSVFilePath();
        // Make sure path is not empty
        if (string.IsNullOrEmpty(csvDir))
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "No CSV file chosen, please fill the CSVPath variable");
            return;
        }

        Log.Info("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Exporting alarms to: " + csvDir);

        char? fieldDelimiter = GetFieldDelimiter();
        if (fieldDelimiter == null || fieldDelimiter == '\0')
            return;

        bool wrapFields = GetWrapFields();

        List<IUAObjectType> allAlarmsInProject = GetAlarmTypeList();

        foreach (var singleAlarmToExport in allAlarmsInProject)
        {
            List<IUAObject> alarmTypesToExport = GetAlarmList(singleAlarmToExport.NodeId);

            // Export CSV only if we have at least one alarm to process
            if (alarmTypesToExport.Count > 0)
            {
                string pathAlarmType = GetBrowsePathFromIuaNode(InformationModel.Get(singleAlarmToExport.NodeId));
                List<string> propertiesFields = new();
                List<string> valuesFields = new();
                // Add standard fields to the alarm
                propertiesFields.Add("Name");
                propertiesFields.Add("Type");
                propertiesFields.Add("Path");
                propertiesFields.Add("Message");
                propertiesFields.Add("LocalizedMessage");
                if (singleAlarmToExport.GetType().IsAssignableTo(typeof(LimitAlarmControllerType)))
                {
                    propertiesFields.Add("MessageHighHighState");
                    propertiesFields.Add("MessageHighState");
                    propertiesFields.Add("MessageLowState");
                    propertiesFields.Add("MessageLowLowState");
                    propertiesFields.Add("LocalizedMessageHighHighState");
                    propertiesFields.Add("LocalizedMessageHighState");
                    propertiesFields.Add("LocalizedMessageLowState");
                    propertiesFields.Add("LocalizedMessageLowLowState");
                }

                // Add any additional custom field to the list
                CheckAlarmProperties(singleAlarmToExport.NodeId, propertiesFields);

                try
                {
                    // Write CSV header
                    using var csvWriter = new CsvFileWriter(Path.Combine(csvDir, singleAlarmToExport.BrowseName + ".csv"))
                    {
                        FieldDelimiter = fieldDelimiter.Value,
                        WrapFields = wrapFields
                    };
                    csvWriter.WriteLine(propertiesFields.ToArray());
                    foreach (var alarm in alarmTypesToExport.Cast<AlarmController>())
                    {
                        valuesFields = new List<string>
                        {
                            alarm.BrowseName,
                            pathAlarmType.Split("/").LastOrDefault(),
                            GetBrowsePathWithoutNodeName(alarm)
                        };
                        foreach (var item in propertiesFields)
                        {
                            switch (item)
                            {
                                case "Name":
                                case "Type":
                                case "Path":
                                case "InputValueArrayIndex":
                                case "InputValueArraySubIndex":
                                    break;

                                case "InputValue":
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.InputValueVariable));
                                    break;

                                case "Message":
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.MessageVariable));
                                    break;

                                case "LocalizedMessage":
                                    valuesFields.Add(ExtractAlarmMessageKey(alarm.LocalizedMessage));
                                    break;

                                case "MessageHighHighState":
                                    valuesFields.Add(ExportAlarmVariableOrValue(((LimitAlarmController)alarm).MessageHighHighStateVariable));
                                    break;

                                case "MessageHighState":
                                    valuesFields.Add(ExportAlarmVariableOrValue(((LimitAlarmController)alarm).MessageHighStateVariable));
                                    break;

                                case "MessageLowState":
                                    valuesFields.Add(ExportAlarmVariableOrValue(((LimitAlarmController)alarm).MessageLowStateVariable));
                                    break;

                                case "MessageLowLowState":
                                    valuesFields.Add(ExportAlarmVariableOrValue(((LimitAlarmController)alarm).MessageLowLowStateVariable));
                                    break;

                                case "LocalizedMessageHighHighState":
                                    valuesFields.Add(ExtractAlarmMessageKey(((LimitAlarmController)alarm).LocalizedMessageHighHighState));
                                    break;

                                case "LocalizedMessageHighState":
                                    valuesFields.Add(ExtractAlarmMessageKey(((LimitAlarmController)alarm).LocalizedMessageHighState));
                                    break;

                                case "LocalizedMessageLowState":
                                    valuesFields.Add(ExtractAlarmMessageKey(((LimitAlarmController)alarm).LocalizedMessageLowState));
                                    break;

                                case "LocalizedMessageLowLowState":
                                    valuesFields.Add(ExtractAlarmMessageKey((alarm as LimitAlarmController).LocalizedMessageLowLowState));
                                    break;

                                case "MaxTimeShelved":
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.MaxTimeShelvedVariable));
                                    break;
                                case "PresetTimeShelved":
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.PresetTimeShelvedVariable));
                                    break;

                                case "LatchingEnabled":
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.LatchingEnabledVariable));
                                    break;

                                default:
                                    valuesFields.Add(ExportAlarmVariableOrValue(alarm.GetVariable(item)));
                                    break;
                            }
                        }
                        // Write CSV content per each alarm
                        csvWriter.WriteLine(valuesFields.ToArray());
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Unable to export alarms: " + ex);
                }
            }
            else
            {
                Log.Info("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "No alarms to export for " + singleAlarmToExport.BrowseName);
            }
        }
        Log.Info("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Alarms successfully exported to " + csvDir);
    }

    private string ExtractAlarmMessageKey(LocalizedText message)
    {
        if (message?.HasTextId == true)
        {
            return message.TextId;
        }
        else
        {
            return string.Empty;
        }
    }

    private string DurationToString(double durationValue)
    {
        return TimeSpan.FromMilliseconds(durationValue).ToString("d'.'hh':'mm':'ss'.'fff");
    }

    private static Regex IsNotOnlyNumberOrBooleanOrDurationValue()
    {
        return new Regex(@"^-?\d+$|^\d\.\d{2}:\d{2}:\d{2}\.\d{3}$|^(True|False)$");
    }

    private static string GetBrowsePathWithoutNodeName(IUANode uaObj)
    {
        var browsePath = GetBrowsePathFromIuaNode(uaObj);
        return browsePath.Contains('/') ? browsePath.Substring(0, browsePath.LastIndexOf("/", StringComparison.Ordinal)) : browsePath;
    }

    private static string GetBrowsePathFromIuaNode(IUANode uaNode) => ClearPathFromProjectInfo(Log.Node(uaNode));

    private static string ClearPathFromProjectInfo(string path)
    {
        var projectName = Project.Current.BrowseName + "/";
        var occurrence = path.IndexOf(projectName);
        if (occurrence == -1)
        {
            return path;
        }
        return path.Substring(occurrence + projectName.Length);
    }

    private string ExportAlarmVariableOrValue(IUAVariable varToAnalyze)
    {
        if (varToAnalyze == null)
            return string.Empty;
        // Get the DynamicLink (variable linked) of the Dynamic Link
        DynamicLink inputPath = (DynamicLink)varToAnalyze.GetVariable("DynamicLink");
        // If inputPath is null, return the value of the variable
        if (inputPath == null)
        {
            if (varToAnalyze.DataType == OpcUa.DataTypes.Duration)
            {
                return DurationToString(varToAnalyze.Value);
            }
            else if (varToAnalyze.Value.Value is LocalizedText localizedTextValue)
            {
                if (localizedTextValue.HasTextId)
                {
                    return localizedTextValue.TextId;
                }
                else
                {
                    return localizedTextValue.Text ?? string.Empty;
                }
            }
            else
            {
                return varToAnalyze.Value ?? string.Empty;
            }
        }

        // Resolve the path of variable linked to the field
        PathResolverResult resolvePathResult = LogicObject.Context.ResolvePath(varToAnalyze, inputPath.Value);
        // If resolvePathResult is null, return empty string
        if (resolvePathResult == null)
            return string.Empty;
        string pathToInputValueNode;
        // Check if is an Alias or Variable
        if (resolvePathResult.AliasSpecification != null && resolvePathResult.AliasSpecification.AliasTokenPath != "")
        {
            // If is alias return the full value of inputPath like {aliasName}\<struct>
            pathToInputValueNode = inputPath.Value;
        }
        else
        {
            // Get the path in string format of the variable for write to CSV file
            pathToInputValueNode = MakeBrowsePath(resolvePathResult.ResolvedNode);
            // if the Indexes is plus then 0, mean is an array (more indexes, more dimension of array)
            if (resolvePathResult.Indexes?.Length > 0)
            {
                StringBuilder stringBracketBuilder = new StringBuilder();
                // Open square brackets for string notation
                _ = stringBracketBuilder.Append("[");
                // for each index append the value on the string with a , as separator
                for (int i = 0; i < resolvePathResult.Indexes.Length; i++)
                {
                    _ = stringBracketBuilder.Append(resolvePathResult.Indexes[i]);
                    // if not the last element add a ,
                    if (i != resolvePathResult.Indexes.Length - 1)
                        _ = stringBracketBuilder.Append(",");
                }
                // Close the square brackets for string notation
                _ = stringBracketBuilder.Append("]");
                pathToInputValueNode += stringBracketBuilder.ToString();
            }
            // Try if the variable is an indexed word
            string dynamicLinkPath = varToAnalyze.GetVariable("DynamicLink").Value.Value.ToString();
            if (Regex.IsMatch(dynamicLinkPath, "\\.\\d*?\\z"))
            {
                var splitPath = dynamicLinkPath.Split(".");
                pathToInputValueNode += "." + splitPath[^1];
            }
        }
        return pathToInputValueNode;
    }

    private static string MakeBrowsePath(IUANode node)
    {
        string path = node.BrowseName;
        IUANode current = node.Owner;

        while (current != Project.Current)
        {
            path = $"{current.BrowseName}/{path}";
            current = current.Owner;
        }
        return path;
    }

    private List<IUAObject> GetAlarmList(NodeId alarmTypeNodeId)
    {
        var alarms = new List<IUAObject>();
        var typedAlarms = GetAlarmsByType(alarmTypeNodeId);
        alarms.AddRange(typedAlarms);
        return alarms;
    }

    private IReadOnlyList<IUAObject> GetAlarmsByType(NodeId type)
    {
        var alarmType = LogicObject.Context.GetObjectType(type);
        var alarms = alarmType.InverseRefs.GetObjects(OpcUa.ReferenceTypes.HasTypeDefinition, false);
        return alarms;
    }

    private string GetCSVFilePath()
    {
        string csvPath;
        FileInfo fi;
        try
        {
            var csvPathVariable = LogicObject.Get<IUAVariable>("CSVPath");
            csvPath = new ResourceUri(csvPathVariable.Value).Uri;
            fi = new FileInfo(csvPath);
        }
        catch (Exception ex)
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Cannot read CSV path, exception: " + ex.Message);
            return string.Empty;
        }
        return fi.DirectoryName;
    }

    private char? GetFieldDelimiter()
    {
        var separatorVariable = LogicObject.GetVariable("CharacterSeparator");
        if (separatorVariable == null)
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "CharacterSeparator variable not found");
            return null;
        }

        string separator = separatorVariable.Value;

        if (separator.Length != 1)
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "Wrong CharacterSeparator configuration. Please insert a char");
            return null;
        }

        if (char.TryParse(separator, out char result))
            return result;

        return null;
    }

    private bool GetWrapFields()
    {
        var wrapFieldsVariable = LogicObject.GetVariable("WrapFields");
        if (wrapFieldsVariable == null)
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, "WrapFields variable not found");
            return false;
        }

        return wrapFieldsVariable.Value;
    }

    private static bool CreateFoldersTreeFromPath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return true;

        var segments = path.Split('/').ToList();
        string updatedSegment = "";
        string segmentsAccumulator = "";
        try
        {
            foreach (var s in segments)
            {
                if (segmentsAccumulator?.Length == 0)
                    updatedSegment = s;
                else
                    updatedSegment = $"{updatedSegment}/{s}";
                var folder = InformationModel.MakeObject<Folder>(s);
                var folderAlreadyExists = Project.Current.GetObject(updatedSegment) != null;
                if (!folderAlreadyExists)
                {
                    if (segmentsAccumulator?.Length == 0)
                        Project.Current.Add(folder);
                    else
                        Project.Current.GetObject(segmentsAccumulator).Children.Add(folder);
                }
                segmentsAccumulator = updatedSegment;
            }
        }
        catch (Exception e)
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Cannot create folder, error {e.Message}");
            return false;
        }
        return true;
    }

    private static CsvUaObject GetDataFromCsvRow(List<string> line, List<string> header)
    {
        var csvUaObject = new CsvUaObject
        {
            Name = line[0],
            TypeBrowsePath = line[1],
            BrowsePath = line[2]
        };

        if (!csvUaObject.IsValid())
        {
            Log.Error("ImportAndExportAlarms." + MethodBase.GetCurrentMethod().Name, $"Invalid object with name {csvUaObject.Name}. Please check its properties.");
            return null;
        }

        for (var i = 3; i < header.Count; i++)
        {
            csvUaObject.Variables.Add(header[i], line[i]);
        }

        return csvUaObject;
    }

    private sealed class CsvUaObject
    {
        public string Name { get; set; }
        public string TypeBrowsePath { get; set; }
        public string BrowsePath { get; set; }
        public Dictionary<string, string> Variables { get; set; } = new Dictionary<string, string>();

        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(TypeBrowsePath) &&
                   !string.IsNullOrWhiteSpace(Name) &&
                   !string.IsNullOrWhiteSpace(BrowsePath);
        }
    }

    private class CsvFileReader : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public bool IgnoreMalformedLines { get; set; } = false;

        public CsvFileReader(string filePath)
        {
            streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8);
        }

        public bool EndOfFile()
        {
            return streamReader.EndOfStream;
        }

        public List<string> ReadLine()
        {
            if (EndOfFile())
                return new List<string>();

            var line = streamReader.ReadLine();

            var result = WrapFields ? ParseLineWrappingFields(line) : ParseLineWithoutWrappingFields(line);

            currentLineNumber++;
            return result;
        }

        private List<string> ParseLineWithoutWrappingFields(string line)
        {
            if (string.IsNullOrEmpty(line))
            {
                if (IgnoreMalformedLines)
                {
                    return new List<string>();
                }
                else
                {
                    throw new FormatException($"Error processing line {currentLineNumber}. Line cannot be empty");
                }
            }
            return line.Split(FieldDelimiter).ToList();
        }

        private List<string> ParseLineWrappingFields(string line)
        {
            var fields = new List<string>();
            var buffer = new StringBuilder("");
            var fieldParsing = false;

            int i = 0;
            while (i < line.Length)
            {
                if (!fieldParsing)
                {
                    if (IsWhiteSpace(line, i))
                    {
                        ++i;
                        continue;
                    }

                    // Line and column numbers must be 1-based for messages to user
                    var lineErrorMessage = $"Error processing line {currentLineNumber}";
                    if (i == 0)
                    {
                        // A line must begin with the quotation mark
                        if (!IsQuoteChar(line, i))
                        {
                            if (IgnoreMalformedLines)
                                return new List<string>();
                            else
                                throw new FormatException($"{lineErrorMessage}. Expected quotation marks at column {i + 1}");
                        }

                        fieldParsing = true;
                    }
                    else
                    {
                        if (IsQuoteChar(line, i))
                        {
                            fieldParsing = true;
                        }
                        else if (!IsFieldDelimiter(line, i))
                        {
                            if (IgnoreMalformedLines)
                                return new List<string>();
                            else
                                throw new FormatException($"{lineErrorMessage}. Wrong field delimiter at column {i + 1}");
                        }
                    }

                    ++i;
                }
                else
                {
                    if (IsEscapedQuoteChar(line, i))
                    {
                        i += 2;
                        buffer.Append(QuoteChar);
                    }
                    else if (IsQuoteChar(line, i))
                    {
                        fields.Add(buffer.ToString());
                        buffer.Clear();
                        fieldParsing = false;
                        ++i;
                    }
                    else
                    {
                        buffer.Append(line[i]);
                        ++i;
                    }
                }
            }

            return fields;
        }

        private bool IsEscapedQuoteChar(string line, int i)
        {
            return line[i] == QuoteChar && i != line.Length - 1 && line[i + 1] == QuoteChar;
        }

        private bool IsQuoteChar(string line, int i)
        {
            return line[i] == QuoteChar;
        }

        private bool IsFieldDelimiter(string line, int i)
        {
            return line[i] == FieldDelimiter;
        }

        private static bool IsWhiteSpace(string line, int i)
        {
            return Char.IsWhiteSpace(line[i]);
        }

        private readonly StreamReader streamReader;
        private int currentLineNumber = 1;

        #region IDisposable support

        private bool disposed = false;

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamReader.Dispose();

            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }

    private class CsvFileWriter : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public CsvFileWriter(string filePath)
        {
            streamWriter = new StreamWriter(filePath, false, System.Text.Encoding.UTF8);
        }

        public void WriteLine(string[] fields)
        {
            var stringBuilder = new StringBuilder();

            for (var i = 0; i < fields.Length; ++i)
            {
                if (WrapFields)
                    stringBuilder.AppendFormat("{0}{1}{0}", QuoteChar, EscapeField(fields[i]));
                else
                    stringBuilder.AppendFormat("{0}", fields[i]);

                if (i != fields.Length - 1)
                    stringBuilder.Append(FieldDelimiter);
            }

            streamWriter.WriteLine(stringBuilder.ToString());
            streamWriter.Flush();
        }

        private string EscapeField(string field)
        {
            var quoteCharString = QuoteChar.ToString();
            return field.Replace(quoteCharString, quoteCharString + quoteCharString);
        }

        private readonly StreamWriter streamWriter;

        #region IDisposable Support

        private bool disposed = false;

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamWriter.Dispose();

            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }

    private static void CheckAlarmProperties(NodeId alarmType, List<string> propertyList)
    {
        IUANode myAlarm = InformationModel.Get(alarmType);
        IUAObjectType myAlarmSuperType = ((UAObjectType)myAlarm).SuperType;
        while (myAlarmSuperType != null)
        {
            if (myAlarmSuperType is AlarmControllerType || myAlarmSuperType is LimitAlarmControllerType)
            {
                propertyList.AddRange(myAlarmSuperType.Children.Where(
                    item => commonProperties.Contains(item.BrowseName) &&
                    item.BrowseName != "LastEvent" &&
                    !propertyList.Contains(item.BrowseName)).Select(item => item.BrowseName));
            }
            myAlarmSuperType = myAlarmSuperType.SuperType;
        }
        foreach (string itemBrowseName in myAlarm.Children.Select(x => x.BrowseName))
        {
            if (propertyList.Contains(itemBrowseName) || itemBrowseName == "LastEvent")
                continue;
            propertyList.Add(itemBrowseName);
        }
    }

    private void CollectRecursive(IUAObjectType parentType, List<IUAObjectType> allControllerTypes)
    {
        allControllerTypes.Add(parentType);
        foreach (var childType in parentType.Refs.GetObjectTypes(OpcUa.ReferenceTypes.HasSubtype, false))
            CollectRecursive(childType, allControllerTypes);
    }
}
