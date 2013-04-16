var edge = require('edge');
if(!edge){
	throw new Error('Edge module not found!');
}
exports.sql = edge.func({
    assemblyFile: __dirname + '\\edge-sql.dll',
    typeName: 'EdgeSql'
});