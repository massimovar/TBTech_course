#region Using directives

using FTOptix.CommunicationDriver;
using FTOptix.Core;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using System.Linq;
using UAManagedCore;

#endregion

public class FromPlcToModel : BaseNetLogic
{
    [ExportMethod]
    public void GenerateNodesIntoModel()
    {
        // Get nodes where to search PLC tags
        startingNodeToFetch = InformationModel.Get(LogicObject.GetVariable("StartingNodeToFetch").Value);
        if (startingNodeToFetch == null)
        {
            Log.Error(GetType().Name, "Cannot get StartingNodeToFetch");
            return;
        }
        // Delete existing nodes if needed
        deleteExistingTags = LogicObject.GetVariable("DeleteExistingTags").Value;
        // Get the node where we are going to create the Model variables/objects
        targetNode = InformationModel.Get<Folder>(LogicObject.GetVariable("TargetFolder").Value);
        if (targetNode == null)
        {
            Log.Error(GetType().Name, "Cannot get TargetNode");
            return;
        }
        // Start procedure
        generateNodesTask = new LongRunningTask(GenerateNodesMethod, LogicObject);
        generateNodesTask.Start();
    }

    private void GenerateNodesMethod()
    {
        GenerateNodes(startingNodeToFetch);
        generateNodesTask?.Dispose();
    }

    private LongRunningTask generateNodesTask;
    private IUANode startingNodeToFetch;
    private IUANode targetNode;
    private bool deleteExistingTags;

    /// <summary>
    /// Generates a set of objects and variables in model in order to have a "copy" of a set of imported tags, retrieved from a starting node
    /// </summary>
    public void GenerateNodes(IUANode startingNode)
    {
        Folder modelFolder = InformationModel.Get<Folder>(LogicObject.GetVariable("TargetFolder").Value);

        if (modelFolder == null)
        {
            Log.Error($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", "Cannot get to target folder");
            return;
        }

        CreateModelTag(startingNode, modelFolder);
        CheckDynamicLinks();
    }

    private void CreateModelTag(IUANode fieldNode, IUANode parentNode, string browseNamePrefix = "")
    {
        switch (fieldNode)
        {
            case TagStructure:
                if (!IsTagStructureArray(fieldNode))
                    CreateOrUpdateObject(fieldNode, parentNode, browseNamePrefix);
                else
                    CreateOrUpdateObjectArray(fieldNode, parentNode);
                break;
            case FTOptix.Core.Folder:
                IUANode newFolder;
                if (fieldNode.NodeId != startingNodeToFetch.NodeId)
                    newFolder = CreateFolder(fieldNode, parentNode);
                else
                    newFolder = parentNode;

                foreach (IUANode children in fieldNode.Children)
                    CreateModelTag(children, newFolder, browseNamePrefix);
                break;
            default:
                CreateOrUpdateVariable(fieldNode, parentNode, browseNamePrefix);
                break;
        }
    }

    private static bool IsTagStructureArray(IUANode fieldNode) => ((TagStructure) fieldNode).ArrayDimensions.Length != 0;

    private IUANode CreateFolder(IUANode fieldNode, IUANode parentNode)
    {
        if (parentNode.Get<FTOptix.Core.Folder>(fieldNode.BrowseName) == null)
        {
            Folder newFolder = InformationModel.Make<FTOptix.Core.Folder>(fieldNode.BrowseName);
            parentNode.Add(newFolder);
            Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Creating \"{Log.Node(newFolder)}\"");
            return newFolder;
        }
        else
        {
            if (deleteExistingTags)
            {
                Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Deleting \"{Log.Node(fieldNode)}\" (DeleteExistingTags is set to True)");
                parentNode.Get<FTOptix.Core.Folder>(fieldNode.BrowseName).Children.Clear();
            }
            else
            {
                Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"\"{Log.Node(fieldNode)}\" already exists, skipping creation or children deletion (DeleteExistingTags is set to False)");
            }

            return parentNode.Get<FTOptix.Core.Folder>(fieldNode.BrowseName);
        }
    }

    private void CreateOrUpdateObjectArray(IUANode fieldNode, IUANode parentNode)
    {
        TagStructure tagStructureArrayTemp = (TagStructure)fieldNode;

        foreach (IUANode tagStructureMember in tagStructureArrayTemp.Children.Where(c => !IsArrayDimensionsVariable(c)))
            CreateModelTag(tagStructureMember, parentNode, fieldNode.BrowseName + "_");
    }

    private void CreateOrUpdateObject(IUANode fieldNode, IUANode parentNode, string browseNamePrefix = "")
    {
        IUAObject existingNode = GetChild<IUAObject>(fieldNode, parentNode, browseNamePrefix);
        // Replacing "/" with "_". Nodes with BrowseName "/" are not allowed
        string filedNodeBrowseName = fieldNode.BrowseName.Replace("/", "_");

        if (existingNode == null)
        {
            existingNode = InformationModel.MakeObject(browseNamePrefix + filedNodeBrowseName);
            parentNode.Add(existingNode);
            Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Creating \"{Log.Node(existingNode)}\" object");
        }
        else
        {
            Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Updating \"{Log.Node(existingNode)}\" object");
        }

        foreach (IUANode childrenNode in fieldNode.Children.Where(c => !IsArrayDimensionsVariable(c)))
            CreateModelTag(childrenNode, existingNode);
    }

    private void CreateOrUpdateVariable(IUANode fieldNode, IUANode parentNode, string browseNamePrefix = "")
    {
        if (IsArrayDimensionsVariable(fieldNode))
        {
            return;
        }
        IUAVariable existingNode = GetChild<IUAVariable>(fieldNode, parentNode, browseNamePrefix);
        IUAVariable fieldTag = (IUAVariable) fieldNode;
        if (existingNode == null)
        {
            // Replacing "/" with "_". Nodes with BrowseName "/" are not allowed
            string tagBrowseName = fieldTag.BrowseName.Replace("/", "_");
            existingNode = InformationModel.MakeVariable(tagBrowseName, fieldTag.DataType, fieldTag.ArrayDimensions);
            parentNode.Add(existingNode);
            Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Creating \"{Log.Node(existingNode)}\" variable");
        }
        else
        {
            Log.Info($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"Updating \"{Log.Node(existingNode)}\" object");
        }
        if (existingNode.DataType != fieldTag.DataType)
        {
            existingNode.DataType = fieldTag.DataType;
        }
        existingNode.SetDynamicLink(fieldTag, FTOptix.CoreBase.DynamicLinkMode.ReadWrite);
    }

    private static bool IsArrayDimensionsVariable(IUANode n) => n.BrowseName.Contains("arraydimen", System.StringComparison.InvariantCultureIgnoreCase);

    private static T GetChild<T>(IUANode child, IUANode parent, string browseNamePrefix = "")
    {
        T returnValue;
        try
        {
            returnValue = (T)parent.Children[browseNamePrefix + child.BrowseName];
        }
        catch
        {
            returnValue = default;
        }
        return returnValue;
    }

    private void CheckDynamicLinks()
    {
        foreach (DynamicLink dataBind in targetNode.FindNodesByType<FTOptix.CoreBase.DynamicLink>())
        {
            if (LogicObject.Context.ResolvePath(dataBind.Owner, dataBind.Value).ResolvedNode == null)
                Log.Warning($"{GetType().Name}.{System.Reflection.MethodBase.GetCurrentMethod().Name}", $"\"{Log.Node(dataBind.Owner)}\" has unresolved databind, you may need to either: manually reimport the missing PLC tag(s), manually delete the unresolved Model variable(s) or set DeleteExistingTags to True (which may lead to unresolved DynamicLinks somewhere else)");
        }
    }
}
