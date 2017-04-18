-- Copyright (C) 2012 Yichun Zhang (agentzh)

local prot = require("protocol")
local socket = require("socket.core")
local bit = require("bit")

local strbyte = string.byte
local concat = table.concat
local tohex = bit.tohex
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor

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

local function _dump(data, isprint)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = tohex(strbyte(data, i), 2)
    end
    if not isprint then print(concat(bytes, " ")) end
    return (concat(bytes, " "))
end

local function _dump_dict(data)
    for k,v in pairs(data) do
        if type(v) == 'string' then
            print('k=' .. k .. ' v=' .. v)
            --print('k=' .. k .. ' v=' .. _dump(v))
        else
            print('k=' .. k .. ' v=' .. tohex(v))
        end
    end
end

--create a database object
function _mysql.new()
    local client_capability_flags = 0x3f7cf
    return setmetatable({client_capability_flags = client_capability_flags}, dbt)
end

--attempts to establish a connection to the given options
function _database.connect(self, opts)
    local ctx = {}

    ctx.client_capability_flags = self.client_capability_flags

    local sock, err = socket.tcp()
    if not sock then 
        print('socket is null')
        return nil, err 
    else
        print('socket is not null')
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

    --print('pkt_type: ' .. pkt_type)
    --_dump(pkt_data)

    if not pkt_type then
        return nil, err
    elseif pkt_type == 'ERR' then
        local err_res = prot.parse_err_packet(pkt_data)
        --_dump_dict(err_res)
        return nil, err_res.sql_state
    end

    local hand_res = prot.parse_handshake_packet(pkt_data)
    --_dump_dict(hand_res)

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
    --print('pkt_type: ' .. pkt_type)
    --_dump(pkt_data)
    if not pkt_type then
        return nil, err
    elseif pkt_type == 'ERR' then
        local err_res = prot.parse_err_packet(pkt_data)
        --_dump_dict(err_res)
        return nil, err_res.sql_state
    elseif pkt_type == 'EOF' then
        return nil, "old pre-4.1 authentication protocol not supported"
    elseif pkt_type ~= 'OK' then
        return nil, "bad packet type: " .. pkt_type
    end

    return setmetatable(ctx, connt)
end

--releases this connection object's database.
function _connection.close(self)
end

--execute a query from sql statement
function _connection.execute(self, sql)
    local ctx = self
    ctx.sequence_id = -1
    local com_query_pkt = prot.construct_com_query_packet(sql)
    local sent_cnt, err = prot.send_packet(ctx, com_query_pkt)
    if not sent_cnt then
        return nil, "failed to send com_query packet: " .. err
    end

    local result_set = {}
    result_set.table_set = {}
    local table_index = 1
    while(true)
    do
        local pkt_type, pkt_data, err = prot.recv_packet(ctx)
        --print('pkt_type: ' .. pkt_type)
        --_dump(pkt_data)
        if not pkt_type then
            return nil, err
        elseif pkt_type == 'ERR' then
            local err_res = prot.parse_err_packet(pkt_data)
            --_dump_dict(err_res)
            return nil, err_res.sql_state
        elseif pkt_type == 'OK' then
            local ok_res = prot.parse_ok_packet(pkt_data)
            return ok_res
        elseif pkt_type ~= 'DATA' then
            return nil, err
        end

        local req_type = band(ctx.client_capability_flags, prot.CLIENT_DEPRECATE_EOF) > 0 and 'OK' or 'EOF'
        --local pkt_type, pkt_data, err = prot.recv_packet(ctx)
        --skip parsing OK or EOF packet

        local col_cnt = prot.parse_column_count_packet(pkt_data)
        --print('col_cnt=' .. col_cnt)
        local col_defs = {}
        for i = 1, col_cnt do
            local pkt_type, pkt_data, err = prot.recv_packet(ctx)
            --print('pkt_type: ' .. pkt_type)
            --_dump(pkt_data)
            local col_def = prot.parse_column_definition_packet(pkt_data)
            --_dump_dict(col_def)
            table.insert(col_defs, col_def)
        end
        --If the CLIENT_DEPRECATE_EOF client capability flag is not set, EOF_Packet
        if req_type == 'EOF' then
            local pkt_type, pkt_data, err = prot.recv_packet(ctx)
            --print('pkt_type: ' .. pkt_type)
            --_dump(pkt_data)
            --skip parsing OK or EOF packet
        end
        result_set.table_set[table_index] = {}
        local row_index = 1
        while(true)
        do
            local pkt_type, pkt_data, err = prot.recv_packet(ctx)
            if pkt_type == 'ERR' then
                local err_res = prot.parse_err_packet(pkt_data)
                return nil, err_res.sql_state
            elseif pkt_type == req_type then
                local ok_res = nil
                if req_type == 'OK' then
                    ok_res = prot.parse_ok_packet(pkt_data)
                elseif req_type == 'EOF' then
                    ok_res = prot.parse_eof_packet(pkt_data)
                end
                --another ProtocolText::Resultset will follow
                if band(ok_res.status_flags, prot.SERVER_MORE_RESULTS_EXISTS) > 0 then
                    --print('more results exists')
                    table_index = table_index + 1
                    break
                end

                return result_set
            elseif pkt_type == 'DATA' then
                --_dump(pkt_data)
                local row = prot.parse_resultset_row_packet(pkt_data, col_defs)
                --_dump_dict(row)
                result_set.table_set[table_index][row_index] = row
                row_index = row_index + 1
            end
        end
    end
    return result_set
end

--creates a PreparedStatement object for sending parameterized SQL statements to the database
function _connection.prepareStatement(self, sql)
    local ctx = self
    local com_stmt_prepare_pkt = prot.construct_com_stmt_prepare_packet(sql)
    local sent_cnt, err = prot.send_packet(ctx, com_stmt_prepare_pkt)
    if not sent_cnt then
        return nil, "failed to send com_stmt_prepare packet: " .. err
    end
    local pkt_type, pkt_data, err = prot.recv_packet(ctx)
    if pkt_type == 'ERR' then
        local err_res = prot.parse_err_packet(pkt_data)
        return nil, err_res.sql_state
    end
    local pstmt_def, err = prot.parse_com_stmt_prepare_ok_packet(pkt_data)
    local param_defs = {}
    if stmt_def.num_params > 0 then
        for i = 1, stmt_def.num_params do
            local pkt_type, pkt_data, err = prot.recv_packet(ctx)
            local param_def = prot.parse_column_definition_packet(pkt_data)
            param_defs[i] = param_def
        end
        local pkt_type, pkt_data, err = prot.recv_packet(ctx)
        --skip parsing EOF packet
    end
    local col_defs = {}
    if stmt_def.num_columns > 0 then
        for i = 1, stmt_def.num_columns do
            local pkt_type, pkt_data, err = prot.recv_packet(ctx)
            local col_def = prot.parse_column_definition_packet(pkt_data)
            col_defs[i] = col_def
        end
        local pkt_type, pkt_data, err = prot.recv_packet(ctx)
        --skip parsing EOF packet
    end

    pstmt_def.param_defs = param_defs
    pstmt_def.col_defs = col_defs
    return setmetatable({ ctx = ctx, pstmt_def = pstmt_def }, pstmt)
end

--sets the designated parameter
function _preparedstatememt.setParameter(self, index, param)
end

--executes the SQL statement in this PreparedStatement object
function _preparedstatememt.execute(self)
    return {}
end

return _mysql