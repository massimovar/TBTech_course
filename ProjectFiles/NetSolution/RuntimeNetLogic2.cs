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

public class RuntimeNetLogic2 : BaseNetLogic
{
    public override void Start()
    {
        Log.Info("Page loaded");
    }

    public override void Stop()
    {
        Log.Info("Page unloaded");
    }
}
