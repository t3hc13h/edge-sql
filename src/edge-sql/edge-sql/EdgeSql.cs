using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;



public class EdgeSql
{
    public async Task<object> Invoke(object i)
    {
        var connectionArgs = i as Dictionary<string, object>;
        string connectionString = null;
        if (connectionArgs != null)
            connectionString = connectionArgs.TryGetObjectValueAs<string>("connectionString");

        return new
        {
            query = (Func<object, Task<object>>)(async (input) =>
            {
                var info = GetExecutionInfo(input as Dictionary<string, object>, connectionString);
                return await this.ExecuteQuery(info);
            }),
            nonQuery = (Func<object, Task<object>>)(async (input) =>
            {
                var info = GetExecutionInfo(input as Dictionary<string, object>, connectionString);
                return await this.ExecuteNonQuery(info);
            }),
            scalar = (Func<object, Task<object>>)(async (input) =>
            {
                var info = GetExecutionInfo(input as Dictionary<string, object>, connectionString);
                return await this.ExecuteScalar(info);
            })
        };
    }

    private ExecutionInfo GetExecutionInfo(Dictionary<string, object> args, string connection = null)
    {
        var info = new ExecutionInfo();
        info.CommandType = GetCommandType(args);
        info.ParametersValues = GetParameterValues(args.TryGetObjectValueAs<object>("commandParameters", true));
        info.CommandText = args.TryGetObjectValueAs<string>("commandText");
        info.ConnectionString = string.IsNullOrEmpty(connection) ? args.TryGetObjectValueAs<string>("connectionString") : connection;
        return info;
    }

    private List<Dictionary<string, object>> GetParameterValues(object p)
    {
        //Inorder to support a few forms on input we'll try and normalize the parameters into a single form

        //Should get this if they pass this:
        //commandParameters: {a:1}
        if (p is Dictionary<string, object>)
        {
            return new[] { p as Dictionary<string, object> }.ToList();
        }
        //Should get this if they pass something like this:
        //commandParameters: [{a:1},{b:2}]
        if (p is object[])
        {
            return (p as object[]).Cast<Dictionary<string, object>>().ToList();
        }
        return null;
    }

    private class ExecutionInfo
    {
        public string ConnectionString { get; set; }
        public string CommandText { get; set; }
        public List<Dictionary<string, object>> ParametersValues { get; set; }
        public CommandType CommandType { get; set; }
    }

    private CommandType GetCommandType(Dictionary<string, object> args)
    {
        object commandTypeString = null;
        args.TryGetValue("commandType", out commandTypeString);
        CommandType type;
        if (Enum.TryParse(commandTypeString as string, true, out type))
            return type;
        return CommandType.Text;
    }

    async Task<object> ExecuteQuery(ExecutionInfo info)
    {
        var results = new List<List<object>>();
        var output = new Dictionary<string, object>();
        using (var connection = new SqlConnection(info.ConnectionString))
        {
            using (var command = GetCommand(info, connection))
            {
                await connection.OpenAsync();
                using (SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                {
                    do
                    {
                        var rows = new List<object>();
                        string[] fieldNames = new string[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            fieldNames[i] = reader.GetName(i);
                        }
                        while (await reader.ReadAsync())
                        {
                            object[] values = new object[fieldNames.Length];
                            reader.GetValues(values);
                            var row = new Dictionary<string, object>();
                            for (int i = 0; i < fieldNames.Length; i++)
                            {
                                row[fieldNames[i]] = values[i];
                            }
                            rows.Add(row);
                        }
                        results.Add(rows);


                    } while (await reader.NextResultAsync());
                    foreach (SqlParameter p in command.Parameters)
                    {
                        if ((p.Direction & ParameterDirection.Output & ParameterDirection.InputOutput) > 0)
                        {
                            output[p.ParameterName] = p.Value;
                        }
                    }
                }
            }
        }
        if (output.Count > 0)
        {
            output.Add("Data", results);
            return output;
        }
        if (results.Count > 1)
        {
            return results;
        }
        else
            return results.FirstOrDefault(); //"Unwrap" single result set instead of returning single value array
    }

    async Task<object> ExecuteScalar(ExecutionInfo info)
    {
        using (SqlConnection connection = new SqlConnection(info.ConnectionString))
        {
            using (SqlCommand command = GetCommand(info, connection))
            {
                await connection.OpenAsync();
                var value = await command.ExecuteScalarAsync();
                return value;
            }
        }
    }

    private SqlCommand GetCommand(ExecutionInfo info, SqlConnection connection)
    {
        var cmd = new SqlCommand(info.CommandText, connection);
        cmd.CommandType = info.CommandType;
        foreach (var parameter in info.ParametersValues)
        {
            foreach (var kvp in parameter)
            {
                cmd.Parameters.Add(GetParameter(kvp));
            }
        }
        return cmd;
    }

    public SqlParameter GetParameter(KeyValuePair<string, object> parameter)
    {
        var parameterProps = parameter.Value as Dictionary<string, object>;
        if (parameterProps != null) //If they have passed in an object then we'll look for a direction
        {
            ParameterDirection pd = ParameterDirection.Input;
            var directionValue = parameterProps.TryGetObjectValueAs<string>("direction", true);
            if (!string.IsNullOrEmpty(directionValue))
                if (!Enum.TryParse<ParameterDirection>(directionValue, true, out pd))
                    pd = ParameterDirection.Input; //Make sure we default to this

            var param = new SqlParameter(parameter.Key, parameterProps.TryGetObjectValueAs<object>("value", true));
            param.Direction = pd;
            return param;
        }
        else
            return new SqlParameter(parameter.Key, parameter.Value);
    }

    async Task<object> ExecuteNonQuery(ExecutionInfo info)
    {
        using (SqlConnection connection = new SqlConnection(info.ConnectionString))
        {
            using (SqlCommand command = GetCommand(info, connection))
            {
                await connection.OpenAsync();
                return await command.ExecuteNonQueryAsync();
            }
        }
    }
}

public static class Extensions
{
    public static T TryGetObjectValueAs<T>(this IDictionary<string, object> collection, string keyInput, bool ignoreCase = false) where T : class
    {
        object obj;
        string keyToFind = keyInput;
        if (ignoreCase)
        {
            keyToFind = collection.Keys.SingleOrDefault(key => string.Equals(key, keyInput, StringComparison.InvariantCultureIgnoreCase));
        }
        if (keyToFind == null)
            return null;
        collection.TryGetValue(keyToFind, out obj);
        return obj as T;
    }
}
