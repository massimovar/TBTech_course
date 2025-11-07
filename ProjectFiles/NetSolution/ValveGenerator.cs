#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using FTOptix.NativeUI;
using FTOptix.WebUI;
using FTOptix.Alarm;
using FTOptix.Recipe;
using FTOptix.EventLogger;
using FTOptix.SQLiteStore;
using FTOptix.Store;
using FTOptix.CODESYS;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.CommunicationDriver;
using FTOptix.Core;
using FTOptix.DataLogger;
using FTOptix.TwinCAT;
#endregion

public class ValveGenerator : BaseNetLogic
{
    [ExportMethod]
    public void GenerateValves()
    {
        Setup();
        // Obtain valves instances
        var valvesFolderID = LogicObject.GetVariable("ValvesFolderID").Value;
        var valvesFolder = InformationModel.Get<Folder>(valvesFolderID);
        if (valvesFolder == null)
        {
            Log.Error("ValveGenerator", "Valves folder not found.");
            return;
        }
        // var valveInstances = valvesFolder.GetNodesByType<Valve>();

        // Obtain valve UI widget
        var valveWidgetID = LogicObject.GetVariable("ValveWidgetID").Value;

        // for every valve instance 
        // 1 generate valve widget
        // 2 set the valve instance to the widget
        // 3 add the widget to the container

        ((RowLayout)Owner).HorizontalGap = 10;

        foreach (var valve in valvesFolder.Children)
        {
            var valveWidget = InformationModel.MakeObject(valve.BrowseName, valveWidgetID);
            valveWidget.SetAlias("ValveInstance", valve.NodeId);
            Owner.Add(valveWidget);
        }
    }

    [ExportMethod]
    public void GenerateVars()
    {
        var myFolder = Project.Current.Get<Folder>("Model/Folder1");
        for (int i = 1; i <= 5; i++)
        {
            var variable = InformationModel.MakeVariable($"Var{i}", OpcUa.DataTypes.Boolean);
            myFolder.Add(variable);
        }
        
    }

    private void Setup()
    {
        foreach (var item in Owner.Children)
        {
            if (item is NetLogicObject) continue;
            item.Delete();
        }
    }
}
