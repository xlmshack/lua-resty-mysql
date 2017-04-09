local mysql = require('framework')

local database = mysql:new()
if not database then
	print('database is null')
else
	print('database is not null')
end