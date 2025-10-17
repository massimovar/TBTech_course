#region Using directives
using System;
using UAManagedCore;
using System.Linq;
using FTOptix.NetLogic;
#endregion

public class AlarmBannerLogic : BaseNetLogic
{
    private const int MIN_ROTATION_TIME = 500;
    private enum MoveDirection { Forward, Backward };

    public override void Start()
    {
        affinityId = LogicObject.Context.AssignAffinityId();

        currentDisplayedAlarm = LogicObject.GetVariable("CurrentDisplayedAlarm");
        currentDisplayedAlarm.Value = NodeId.Empty;

        currentDisplayedAlarmIndex = LogicObject.GetVariable("CurrentDisplayedAlarmIndex");
        currentDisplayedAlarmIndex.Value = 0;

        RegisterObserverOnLocalizedAlarmsContainer(LogicObject.Context);
        RegisterObserverOnSessionActualLanguageChange(LogicObject.Context);
        RegisterObserverOnLocalizedAlarmsObject(LogicObject.Context);

        retainedAlarmsCount = LogicObject.GetVariable("AlarmCount");
        retainedAlarmsCount.Value = localizedAlarmsContainer?.Children.Count ?? 0;

        rotationTime = LogicObject.GetVariable("RotationTime");
        rotationTime.VariableChange += (_, __) => RestartRotationAndMoveAlarmLock(MoveDirection.Forward);

        RestartRotationAndMoveAlarmLock(MoveDirection.Forward);
    }

    public override void Stop()
    {
        alarmEventRegistration?.Dispose();
        alarmEventRegistration2?.Dispose();
        sessionActualLanguageRegistration?.Dispose();
        rotationTask?.Dispose();

        alarmEventRegistration = null;
        alarmEventRegistration2 = null;
        sessionActualLanguageRegistration = null;
        rotationTask = null;
    }

    #region Exported user methods
    [ExportMethod]
    public void NextAlarm()
    {
        RestartRotationAndMoveAlarmLock(MoveDirection.Forward);
    }

    [ExportMethod]
    public void PreviousAlarm()
    {
        RestartRotationAndMoveAlarmLock(MoveDirection.Backward);
    }
    #endregion

    #region Alarms specific events
    private void OnAlarmAdded(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
    {
        lock (_timerLock)
        {
            retainedAlarmsCount.Value = localizedAlarmsContainer?.Children.Count ?? 0;
            if (currentDisplayedAlarm.Value == UAValue.Null)
            {
                // Set any initial value to prevent multiple calls in the following scenario:
                // 1. First alarm is added and because currentDisplayedAlarm.Value is not set yet the RestartRotationTaskAndMoveAlarm is executed.
                // 2. Then before the periodic task is started, so currentDisplayedAlarm.Value is still not set,
                // another alarm is added and the RestartRotationTaskAndMoveAlarm is executed again.
                // This situation will not lead to an error but banner will start showing e.g. second alarm instead of the first one.
                currentDisplayedAlarm.Value = localizedAlarmsContainer?.Children.ElementAtOrDefault<IUANode>(0)?.NodeId ?? NodeId.Empty;
                RestartRotationAndMoveAlarm(MoveDirection.Forward);
            }
        }
    }

    private void OnAlarmRemoved(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
    {
        lock (_timerLock)
        {
            retainedAlarmsCount.Value = localizedAlarmsContainer?.Children.Count ?? 0;
            if ((int)retainedAlarmsCount.Value == 0 || targetNode.NodeId == (NodeId)currentDisplayedAlarm.Value)
                RestartRotationAndMoveAlarm(MoveDirection.Forward);
        }
    }
    #endregion

    #region AlarmBanner iterates alarms list
    private void MoveAlarm(MoveDirection moveDirection)
    {
        if (moveDirection == MoveDirection.Forward)
            IncrementAlarmIndex();
        else
            DecrementAlarmIndex();
        GoToCurrentAlarm();
    }

    private void IncrementAlarmIndex()
    {
        alarmIndex++;
        int alarmsCount = localizedAlarmsContainer?.Children.Count ?? 0;
        if (alarmsCount == 0)
            alarmIndex = -1;
        else if (alarmIndex >= alarmsCount)
            alarmIndex = 0; // ensure endless loop over alarms
    }

    private void DecrementAlarmIndex()
    {
        alarmIndex--;
        if (alarmIndex < 0)
            alarmIndex = (localizedAlarmsContainer?.Children.Count ?? 0) - 1; // ensure endless loop over alarms
        if (alarmIndex < 0)
            alarmIndex = 0;
    }

    private void GoToCurrentAlarm()
    {
        IUANode alarm = localizedAlarmsContainer?.Children.ElementAtOrDefault<IUANode>(alarmIndex);
        if (alarmIndex > 0 && alarm == null)
        {
            alarmIndex = 0; // reset in case moving to currrent alarm is no longer possible
            alarm = localizedAlarmsContainer?.Children.ElementAtOrDefault<IUANode>(alarmIndex);
        }
        if (alarm != null)
            currentDisplayedAlarm.Value = alarm.NodeId;
        else
            currentDisplayedAlarm.Value = NodeId.Empty;
        currentDisplayedAlarmIndex.Value = alarmIndex;
    }
    #endregion

    #region Alarm observers
    private void RegisterObserverOnLocalizedAlarmsObject(IContext context)
    {
        var retainedAlarms = context.GetNode(FTOptix.Alarm.Objects.RetainedAlarms);

        retainedAlarmsObjectObserver = new RetainedAlarmsObjectObserver((ctx) => RegisterObserverOnLocalizedAlarmsContainer(ctx));

        alarmEventRegistration2 = retainedAlarms.RegisterEventObserver(
            retainedAlarmsObjectObserver, EventType.ForwardReferenceAdded, affinityId);
    }

    private void RegisterObserverOnLocalizedAlarmsContainer(IContext context)
    {
        var retainedAlarms = context.GetNode(FTOptix.Alarm.Objects.RetainedAlarms);
        var localizedAlarmsVariable = retainedAlarms.GetVariable("LocalizedAlarms");
        var localizedAlarmsNodeId = (NodeId)localizedAlarmsVariable.Value;
        if (localizedAlarmsNodeId != null && !localizedAlarmsNodeId.IsEmpty)
            localizedAlarmsContainer = LogicObject.Context.GetNode(localizedAlarmsNodeId);

        if (alarmEventRegistration != null)
        {
            alarmEventRegistration.Dispose();
            alarmEventRegistration = null;
        }

        var alarmsAddRemoveObserver = new AlarmsObserver(this);
        alarmEventRegistration = localizedAlarmsContainer?.RegisterEventObserver(
            alarmsAddRemoveObserver,
            EventType.ForwardReferenceAdded |
            EventType.ForwardReferenceRemoved,
            affinityId);
    }

    private void RegisterObserverOnSessionActualLanguageChange(IContext context)
    {
        var currentSessionActualLanguage = context.Sessions.CurrentSessionInfo.SessionObject.Children["ActualLanguage"];

        sessionActualLanguageChangeObserver = new CallbackVariableChangeObserver(
            (IUAVariable variable, UAValue newValue, UAValue oldValue, uint[] indexes, ulong senderId) =>
            {
                RegisterObserverOnLocalizedAlarmsContainer(context);
            });

        sessionActualLanguageRegistration = currentSessionActualLanguage.RegisterEventObserver(
            sessionActualLanguageChangeObserver, EventType.VariableValueChanged, affinityId);
    }

    private class RetainedAlarmsObjectObserver : IReferenceObserver
    {
        public RetainedAlarmsObjectObserver(Action<IContext> action)
        {
            registrationCallback = action;
        }

        public void OnReferenceAdded(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
        {
            string localeId = targetNode.Context.Sessions.CurrentSessionHandler.ActualLocaleId;
            if (String.IsNullOrEmpty(localeId))
                localeId = "en-US";

            if (targetNode.BrowseName == localeId)
                registrationCallback(targetNode.Context);
        }

        public void OnReferenceRemoved(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
        {
        }

        private Action<IContext> registrationCallback;
    }

    private void RestartRotationAndMoveAlarmLock(MoveDirection direction)
    {
        lock (_timerLock)
        {
            RestartRotationAndMoveAlarm(direction);
        }
    }

    private void RestartRotationAndMoveAlarm(MoveDirection direction)
    {
        rotationTask?.Cancel();
        MoveDirection currentDirection = direction;
        void MoveAlarmInternalPeriodic()
        {
            lock (_alarmLock)
            {
                MoveAlarm(currentDirection);
                currentDirection = MoveDirection.Forward;
            }
        }
        rotationTask = new PeriodicTask(MoveAlarmInternalPeriodic, RotationTimeVerification(rotationTime.Value), LogicObject);
        rotationTask.Start();
    }

    private int RotationTimeVerification(int rotationTimeMilliseconds)
    {
        int returnRotationTime;
        if (rotationTimeMilliseconds < MIN_ROTATION_TIME)
        {
            if (rotationTimePreviousValue != rotationTimeMilliseconds)
                Log.Warning("AlarmBanner", $"Rotation interval is too low: {rotationTimeMilliseconds}[ms]. Setting minimal rotation interval {MIN_ROTATION_TIME}[ms].");
            returnRotationTime = MIN_ROTATION_TIME;
        }
        else
        {
            returnRotationTime = rotationTimeMilliseconds;
        }
        rotationTimePreviousValue = rotationTimeMilliseconds;
        return returnRotationTime;
    }

    private class AlarmsObserver : IReferenceObserver
    {
        public AlarmsObserver(AlarmBannerLogic _alarmsObject)
        {
            alarmsObject = _alarmsObject;
        }

        public void OnReferenceAdded(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
        {
            alarmsObject.OnAlarmAdded(sourceNode, targetNode, referenceTypeId, senderId);
        }
        public void OnReferenceRemoved(IUANode sourceNode, IUANode targetNode, NodeId referenceTypeId, ulong senderId)
        {
            alarmsObject.OnAlarmRemoved(sourceNode, targetNode, referenceTypeId, senderId);
        }
        private AlarmBannerLogic alarmsObject;
    }
    #endregion

    private uint affinityId;
    private IEventRegistration alarmEventRegistration;
    private IEventRegistration alarmEventRegistration2;
    private IEventRegistration sessionActualLanguageRegistration;
    private RetainedAlarmsObjectObserver retainedAlarmsObjectObserver;
    private IEventObserver sessionActualLanguageChangeObserver;
    private IUANode localizedAlarmsContainer = null;
    private PeriodicTask rotationTask;
    private int alarmIndex = -1;
    private IUAVariable retainedAlarmsCount;
    private IUAVariable currentDisplayedAlarm;
    private IUAVariable currentDisplayedAlarmIndex;
    private IUAVariable rotationTime;
    private int rotationTimePreviousValue = -1;
    private readonly object _alarmLock = new object();
    private readonly object _timerLock = new object();
}
