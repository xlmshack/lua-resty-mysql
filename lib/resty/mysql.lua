-- Copyright (C) 2012 Yichun Zhang (agentzh)

local prot = require("protocol")

local _mysql = { _VERSION = '0.16' }
local _database = { _VERSION = '0.16' }
local _connection = { _VERSION = '0.16' }
local _preparedstatememt = { _VERSION = '0.16' }

local myt = { __index = _mysql }
local dbt = { __index = _database }
local connt = { __index = _connection }
local pstmt = { __index == _preparedstatememt }

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

--column types
local MYSQL_TYPE_DECIMAL = 0x00
local MYSQL_TYPE_TINY = 0x01
local MYSQL_TYPE_SHORT = 0x02
local MYSQL_TYPE_LONG = 0x03
local MYSQL_TYPE_FLOAT = 0x04
local MYSQL_TYPE_DOUBLE = 0x05
local MYSQL_TYPE_NULL =  0x06
local MYSQL_TYPE_TIMESTAMP = 0x07
local MYSQL_TYPE_LONGLONG =  0x08
local MYSQL_TYPE_INT24 = 0x09
local MYSQL_TYPE_DATE =  0x0a
local MYSQL_TYPE_TIME  = 0x0b
local MYSQL_TYPE_DATETIME  = 0x0c
local MYSQL_TYPE_YEAR =  0x0d
local MYSQL_TYPE_NEWDATE = 0x0e
local MYSQL_TYPE_VARCHAR  =  0x0f
local MYSQL_TYPE_BIT  =  0x10
local MYSQL_TYPE_TIMESTAMP2 = 0x11
local MYSQL_TYPE_DATETIME2 = 0x12
local MYSQL_TYPE_TIME2 = 0x13
local MYSQL_TYPE_NEWDECIMAL = 0xf6
local MYSQL_TYPE_ENUM =  0xf7
local MYSQL_TYPE_SET  =  0xf8
local MYSQL_TYPE_TINY_BLOB = 0xf9
local MYSQL_TYPE_MEDIUM_BLOB  =  0xfa
local MYSQL_TYPE_LONG_BLOB = 0xfb
local MYSQL_TYPE_BLOB =  0xfc
local MYSQL_TYPE_VAR_STRING = 0xfd
local MYSQL_TYPE_STRING = 0xfe


--create a database object
function _mysql.new()
    local client_capability_flags = 0x3f7cf
    return setmetatable({client_capability_flags = client_capability_flags}, dbt)
end

--attempts to establish a connection to the given options
function _database.connect(self, opts)
    local ctx = {}

    ctx.client_capability_flags = self.client_capability_flags

    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    ctx.sock = sock

    local max_packet_size = opts.max_packet_size
    if not max_packet_size then
        max_packet_size = 1024 * 1024 -- default 1 MB
    end
    ctx.max_packet_size = max_packet_size
    ctx.compact = opts.compact_arrays or 0
    local database = opts.database or ""
    local user = opts.user or "root"
    local pool = opts.pool
    local host = opts.host or 'localhost'
    local port = opts.port or 3306

    if not pool then
        pool = user .. ":" .. database .. ":" .. host .. ":" .. port
    end

    local conn_res, err = sock:connect(host, port)
    if not conn_res then
        return nil, 'failed to connect: ' .. err
    end

    local pkt_type, pkt_data, err = prot.recv_packet(ctx)
    if not pkt_type then
        return nil, err
    elseif pkt_type == 'ERR' then
        local err_res = prot.parse_err_packet(pkt_data)
        return nil, err_res.sql_state
    end

    local hand_res = prot.parse_handshake_packet(pkt_data)

    if opts.ssl_verify then
        if band(hand_res.capability_flags, prot.CLIENT_SSL) == 0 then
            return nil, 'ssl disabled on server'
        end

        local res, err = sock:sslhandshake(false, nil, opts.ssl_verify)
        if not res then
            return nil, "failed to do ssl handshake: " .. (err or "")
        end
    end

    local password = opts.password or ""
    local token = prot.compute_token(password, hand_res.auth_plugin_data_part)

    local handshake_resp_data = {
        capability_flags = ctx.client_capability_flags,
        max_packet_size = ctx.max_packet_size,
        character_set = 0,
        username = user,
        auth_response = token,
        database = database
    }

    local handshake_resp_pkt = prot.construct_handshake_response_packet(handshake_resp_data)

    local sent_cnt, err = prot.send_packet(ctx, handshake_resp_pkt)
    if not sent_cnt then
        return nil, "failed to send client authentication packet: " .. err
    end

    local pkt_type, pkt_data, err = prot.recv_packet(ctx)

    if not pkt_type then
        return nil, err
    elseif pkt_type == 'ERR' then
        local err_res = prot.parse_err_packet(pkt_data)
        return nil, err_res.sql_state
    elseif pkt_type == 'EOF' then
        return nil, "old pre-4.1 authentication protocol not supported"
    elseif  ~= 'OK' then
        return nil, "bad packet type: " .. pkt_type
    end

    return setmetatable(ctx, connt)
end

--releases this connection object's database.
function _connection.close(self)
end

--execute a query from sql statement
function _connection.execute(self, sql)
    return {}
end

--creates a PreparedStatement object for sending parameterized SQL statements to the database
function _connection.prepareStatement(self, sql)
    return setmetatable({ sock = self.sock }, pstmt)
end

--sets the designated parameter
function _preparedstatememt.setParameter(self, index, param)
end

--executes the SQL statement in this PreparedStatement object
function _preparedstatememt.execute(self)
    return {}
end