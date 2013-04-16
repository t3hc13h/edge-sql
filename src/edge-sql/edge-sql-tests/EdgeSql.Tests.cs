using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

[TestClass]
public class UnitTest1
{
    [TestMethod]
    public async Task CanRunQuery()
    {
        var s = new EdgeSql();
        var args = new Dictionary<string, object>();
        args.Add("commandText", "select * from Albums where AlbumId = @Id");
        args.Add("commandType", null);
        args.Add("connectionString", "Server=.\\SQLEXPRESS;Database=musicstore;Integrated Security=SSPI");
        var param = new Dictionary<string, object>();
        param.Add("Id", 1);
        args.Add("commandParameters", param);
        var response = await s.Invoke(args);
        Assert.IsNotNull(response);
    }

    [TestMethod]
    public void CanBuildInputParameter()
    {
        var s = new EdgeSql();
        var pval = new KeyValuePair<string, object>("Key", "Heres the value");
        SqlParameter result = s.GetParameter(pval);
        Assert.AreEqual(result.Value, pval.Value);
    }

    [TestMethod]
    public void CanBuildOuputParameter()
    {
        var s = new EdgeSql();
        var param = new Dictionary<string, object>();
        param.Add("Direction", "output");
        param.Add("Value", "Heres the value");
        var pval = new KeyValuePair<string, object>("Key", param);
        SqlParameter result = s.GetParameter(pval);
        Assert.AreEqual(result.Value, param["Value"]);
        Assert.AreEqual(ParameterDirection.Output, result.Direction);
    }

    [TestMethod]
    public void CanBuildInputOuputParameter()
    {
        var s = new EdgeSql();
        var param = new Dictionary<string, object>();
        param.Add("Direction", "InputOutput");
        param.Add("Value", "Heres the value");
        var pval = new KeyValuePair<string, object>("Key", param);
        SqlParameter result = s.GetParameter(pval);
        Assert.AreEqual(result.Value, param["Value"]);
        Assert.AreEqual(ParameterDirection.InputOutput, result.Direction);
    }
}

