local bit = require("bit")
local sha1 = require("sha1")
local math = require("math")
local conv = require('conversion')

local sub = string.sub
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local format = string.format
local strrep = string.rep
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex
--local sha1 = ngx.sha1_bin
local concat = table.concat
local unpack = unpack
local setmetatable = setmetatable
local error = error
local tonumber = tonumber

local _M = { _VERSION = '0.19' }

local CLIENT_LONG_PASSWORD = 0x00000001
local CLIENT_FOUND_ROWS = 0x00000002
local CLIENT_LONG_FLAG = 0x00000004
local CLIENT_CONNECT_WITH_DB = 0x00000008
local CLIENT_NO_SCHEMA = 0x00000010
local CLIENT_COMPRESS = 0x00000020
local CLIENT_ODBC = 0x00000040
local CLIENT_LOCAL_FILES = 0x00000080
local CLIENT_IGNORE_SPACE = 0x00000100
local CLIENT_PROTOCOL_41 = 0x00000200
local CLIENT_INTERACTIVE = 0x00000400
local CLIENT_SSL = 0x00000800
local CLIENT_IGNORE_SIGPIPE = 0x00001000
local CLIENT_TRANSACTIONS = 0x00002000
local CLIENT_RESERVED = 0x00004000
local CLIENT_SECURE_CONNECTION = 0x00008000
local CLIENT_MULTI_STATEMENTS = 0x00010000
local CLIENT_MULTI_RESULTS = 0x00020000
local CLIENT_PS_MULTI_RESULTS = 0x00040000
local CLIENT_PLUGIN_AUTH = 0x00080000
local CLIENT_CONNECT_ATTRS = 0x00100000
local CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
local CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x00400000
local CLIENT_SESSION_TRACK = 0x00800000
local CLIENT_DEPRECATE_EOF = 0x01000000

_M.CLIENT_SSL = CLIENT_SSL
_M.CLIENT_DEPRECATE_EOF = CLIENT_DEPRECATE_EOF

local SERVER_MORE_RESULTS_EXISTS =  0x0008

_M.SERVER_MORE_RESULTS_EXISTS = SERVER_MORE_RESULTS_EXISTS

local COM_QUERY = 0x03
local COM_STMT_PREPARE = 0x16
local COM_STMT_EXECUTE = 0x17
local COM_STMT_CLOSE = 0x19
local COM_STMT_RESET = 0x1a

local CURSOR_TYPE_NO_CURSOR = 0x00

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local function _dump(data)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = tohex(strbyte(data, i), 2)
    end
    return concat(bytes, " ")
end

local function recv_packet(ctx)
    local sock = ctx.sock

    local data, err = sock:receive(4) --packet header

    if not data then
        return nil, nil, 'receive packet header failed'
    end

    local pkt_len, pos = conv.byte3_to_integer(data, 1) --packet length

    if pkt_len == 0 then
        return nil, nil, 'receive empty packet'
    end

    if pkt_len > ctx.max_packet_size then
        return nil, nil, "packet size too big"
    end

    local seq_id, pos = conv.byte1_to_integer(data, pos) --packet sequence id

    ctx.sequence_id = seq_id

    local payload, err = sock:receive(pkt_len) --packet payload

    if not payload then
        return nil, nil, "failed to read packet content"
    end

    local payload_header, pos = conv.byte1_to_integer(payload, 1)

    if payload_header == 0x00 then
        return 'OK', payload
    elseif payload_header == 0xff then
        return 'ERR', payload
    elseif payload_header == 0xfe then
        return 'EOF', payload
    elseif payload_header < 0xfb then
        return 'DATA', payload
    else
        return nil, nil, 'unknown packet'
    end
end

_M.recv_packet = recv_packet

local function send_packet(ctx, data)
    local sock = ctx.sock

    local send_data = conv.integer_to_byte3(#data) --payload_length:int<3>
    ctx.sequence_id = ctx.sequence_id + 1
    send_data = send_data .. conv.integer_to_byte1(ctx.sequence_id) --sequence_id:int<1>
    send_data = send_data .. data

    return sock:send(send_data)
end

_M.send_packet = send_packet

local function reset_sequence_id(ctx)
    ctx.sequence_id = -1
end

_M.reset_sequence_id = reset_sequence_id

local function parse_err_packet(data)
    local pkt_header, pos = conv.byte1_to_integer(data, 1) --header:int<1>
    local error_code, pos = conv.byte2_to_integer(data, pos) --error_code:int<2>

    local sql_state_marker = sub(data, pos, pos) --sql_state_marker:string<1>
    pos = pos + 1
    local sql_state = nil
    if sql_state_marker == '#' then
        sql_state = sub(data, pos, pos + 5 - 1) --sql_state:string<5>
        pos = pos + 5
    end

    local error_message = sub(data, pos) --error_message:string<EOF>

    local res = {
        error_code = error_code,
        sql_state = sql_state,
        error_message = error_message
    }

    return res
end

_M.parse_err_packet = parse_err_packet

local function parse_ok_packet(data)
    local pkt_header, pos = conv.byte1_to_integer(data, 1) --header:int<1>
    local affected_rows, pos = conv.lenenc_to_integer(data, pos) --affected_rows:int<lenenc>
    local last_insert_id, pos = conv.lenenc_to_integer(data, pos) --last_insert_id:int<lenenc>
    local status_flags, pos = conv.byte2_to_integer(data, pos) --status_flags:int<2>
    local warnings, pos = conv.byte2_to_integer(data, pos) --warnings:int<2>

    local info = sub(data, pos) --info:string<EOF>

    local res = {
        affected_rows = affected_rows,
        last_insert_id = last_insert_id,
        status_flags = status_flags,
        warnings = warnings
    }

    return res
end

_M.parse_ok_packet = parse_ok_packet

local function parse_eof_packet(data)
    local pkt_header, pos = conv.byte1_to_integer(data, 1) --header:int<1>
    local warnings, pos = conv.byte2_to_integer(data, pos) --warnings:int<2>
    local status_flags, pos = conv.byte2_to_integer(data, pos) --status_flags:int<2>

    local res = {
        warnings = warnings,
        status_flags = status_flags
    }

    return res
end

_M.parse_eof_packet = parse_eof_packet

local function parse_handshake_packet(data)
    local res, pos = {}
    res.protocol_ver, pos = conv.byte1_to_integer(data, 1) --protocol_version:[0a]
    res.server_ver, pos = conv.cstr_to_str(data, pos) --server_version:string<NUL>
    res.connection_id, pos = conv.byte4_to_integer(data, pos) --connection_id:int<4>
    res.auth_plugin_data_part = sub(data, pos, pos + 8 - 1) --auth-plugin-data-part-1:string<8>
    pos = pos + 8
    pos = pos + 1 --filler:[00]
    res.capability_flags, pos = conv.byte2_to_integer(data, pos) --capability_flags(lower 2 bytes):int<2>
    --if more data in the packet
    if #data >= pos then
        res.character_set, pos = conv.byte1_to_integer(data, pos) --character_set:int<1>
        res.status_flags, pos = conv.byte2_to_integer(data, pos)
        local capability_flags_upper, pos = conv.byte2_to_integer(data, pos) --capability_flags(upper 2 bytes):int<2>
        res.capability_flags = bor(res.capability_flags, lshift(capability_flags_upper, 16))
        local auth_plugin_data_part_len, pos = conv.byte1_to_integer(data, pos) --auth_plugin_data_part_len:int<1>
        pos = pos + 10 --reserved:string<10>
        if band(res.capability_flags, CLIENT_SECURE_CONNECTION) > 0 then
            auth_plugin_data_part_len = 13 > auth_plugin_data_part_len - 8 and 13 or auth_plugin_data_part_len - 8
            res.auth_plugin_data_part = res.auth_plugin_data_part .. sub(data, pos, pos + auth_plugin_data_part_len - 1 - 1) --auth_plugin_data_part_2:string<$len>, but 
            pos = pos + auth_plugin_data_part_len
        end
        if band(res.capability_flags, CLIENT_PLUGIN_AUTH) > 0 then
            if sub(data, #data) == '\0' then --Bug#59453
                res.auth_plugin_name, pos = conv.cstr_to_str(data, pos)
            else
                res.auth_plugin_name = sub(data, pos)
            end
        end
   end

   return res
end

_M.parse_handshake_packet = parse_handshake_packet

local function construct_handshake_response_packet(client)
    local data = conv.integer_to_byte4(client.capability_flags) --capability_flags:int<4>
    data = data .. conv.integer_to_byte4(client.max_packet_size) --max_packet_size:int<4>
    data = data .. conv.integer_to_byte1(client.character_set) --character_set:int<1>
    data = data .. strrep('\0', 23) --reserved:string<23>
    data = data .. conv.str_to_cstr(client.username) --username:string<NUL>
    if band(client.capability_flags, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) > 0 then
        data = data .. conv.integer_to_lenenc(#client.auth_response)
        data = data .. client.auth_response --auth_response:string<lenenc>
    elseif band(client.capability_flags, CLIENT_SECURE_CONNECTION) > 0 then
        data = data .. conv.integer_to_byte1(#client.auth_response) --auth_response:int<1>
        data = data .. client.auth_response
    else
        data = data .. conv.str_to_cstr(client.auth_response) --auth_response:string<NUL>
    end

    if band(client.capability_flags, CLIENT_CONNECT_WITH_DB) > 0 then
        data = data .. conv.str_to_cstr(client.database) --database:string<NUL>
    end

    if band(client.capability_flags, CLIENT_PLUGIN_AUTH) > 0 then
        data = data .. conv.str_to_cstr(client.auth_plugin_name) --auth_plugin_name:string<NUL>
    end

    if band(client.capability_flags, CLIENT_CONNECT_ATTRS) > 0 then
        if client.connect_attrs and #client.connect_attrs > 0 then
            local all_attr_len = 0
            local attr_data = ''
            for k,v in pairs(client.connect_attrs) do
                attr_data = attr_data .. conv.integer_to_lenenc(#k)
                attr_data = attr_data .. k
                attr_data = attr_data .. conv.integer_to_lenenc(#v)
                attr_data = attr_data .. v
                all_attr_len = all_attr_len + #k + #v
            end
            data = data .. conv.integer_to_lenenc(all_attr_len) .. attr_data
        end 
    end

    return data
end

_M.construct_handshake_response_packet = construct_handshake_response_packet

local function construct_ssl_request_packet(client)
    local data = conv.integer_to_byte4(client.capability_flags) --capability_flags:int<4>
    data = data .. conv.integer_to_byte4(client.max_packet_size) --max_packet_size:int<4>
    data = data .. conv.integer_to_byte1(client.character_set) --character_set:int<1>
    data = data .. strrep('\0', 23) --reserved:string<23>

    return data
end

local function construct_com_query_packet(querystr)
    local send_data = conv.integer_to_byte1(COM_QUERY) --COM_QUERY:int<1>
    send_data = send_data .. querystr --querystr:string<EOF>

    return send_data
end

_M.construct_com_query_packet = construct_com_query_packet

local function parse_column_count_packet(data)
    return conv.lenenc_to_integer(data, 1)
end

_M.parse_column_count_packet = parse_column_count_packet

local function parse_column_definition_packet(data)
    local res, pos = {}
    res.catalog, pos = conv.lenenc_str_to_str(data, 1) --catalog:string<lenenc>
    res.schema, pos = conv.lenenc_str_to_str(data, pos)
    res.table, pos = conv.lenenc_str_to_str(data, pos)
    res.org_table, pos = conv.lenenc_str_to_str(data, pos)
    res.name, pos = conv.lenenc_str_to_str(data, pos)
    res.org_name, pos = conv.lenenc_str_to_str(data, pos)
    res.next_length, pos = conv.lenenc_to_integer(data, pos)
    res.character_set, pos = conv.byte2_to_integer(data, pos)
    res.column_length, pos = conv.byte4_to_integer(data, pos)
    res.type, pos = conv.byte1_to_integer(data, pos)
    res.flags, pos = conv.byte2_to_integer(data, pos)
    res.decimals, pos = conv.byte1_to_integer(data, pos)
    pos = pos + 2 --filler:int<2>

    return res
end

_M.parse_column_definition_packet = parse_column_definition_packet

local function parse_resultset_row_packet(data, column_defs)
    local res, pos, i = {}, 1, 1
    local field_value = ''
    for k,v in pairs(column_defs) do
        field_value, pos = conv.lenenc_str_to_str(data, pos)
        if not field_value then
            res[column_defs[i].name] = nil
        elseif conv.ProtocolTextConverters[column_defs[i].type] then
            res[column_defs[i].name] = conv.ProtocolTextConverters[column_defs[i].type](field_value)
        else
            res[column_defs[i].name] = field_value
        end
        i = i + 1
    end

    return res
end

_M.parse_resultset_row_packet = parse_resultset_row_packet

local function construct_com_stmt_prepare_packet(querystr)
    local send_data = conv.integer_to_byte1(COM_STMT_PREPARE)
    send_data = send_data .. querystr

    return send_data
end

_M.construct_com_stmt_prepare_packet = construct_com_stmt_prepare_packet

local function parse_com_stmt_prepare_ok_packet(data)
    local res, pos = {}
    res.status, pos = conv.byte1_to_integer(data, 1)
    res.statement_id, pos = conv.byte4_to_integer(data, pos)
    res.num_columns, pos = conv.byte2_to_integer(data, pos)
    res.num_params, pos = conv.byte2_to_integer(data, pos)
    pos = pos + 1 --filler:int<1>
    res.warning_count, pos = conv.byte2_to_integer(data, pos)

    return res
end

_M.parse_com_stmt_prepare_ok_packet = parse_com_stmt_prepare_ok_packet

local function construct_com_stmt_execute_packet(stmt, new_params)
    local send_data = conv.integer_to_byte1(COM_STMT_EXECUTE)
    send_data = send_data .. conv.integer_to_byte4(stmt.statement_id)
    send_data = send_data .. conv.integer_to_byte1(CURSOR_TYPE_NO_CURSOR)
    send_data = send_data .. conv.integer_to_byte4(1) --The iteration-count is always 1

    local null_bitmap = {}
    local null_bitmap_len = (stmt.num_params + 7) / 8
    
    for i = 1, null_bitmap_len do
        null_bitmap[i] = 0
    end

    for i = 1, stmt.num_params do
        if not new_params[i]
        or not new_params[i].bin_data then
            local byte_pos = math.modf((i - 1) / 8) + 1
            local bit_pos = (i - 1) % 8
            null_bitmap[byte_pos] = bor(null_bitmap[byte_pos], lshift(1, bit_pos))
        end
    end

    local null_bitmap_data = ''
    for k, v in pairs(null_bitmap) do
        null_bitmap_data = null_bitmap_data .. conv.integer_to_byte1(v)
    end

    send_data = send_data .. null_bitmap_data

    send_data = send_data .. conv.integer_to_byte1(1)

    local value_data = ''
    local type_data = ''
    for i = 1, stmt.num_params do
        if  new_params[i]
        and new_params[i].bin_data then
            type_data = type_data .. conv.integer_to_byte2(new_params[i].bin_type)
            value_data = value_data .. new_params[i].bin_data
        end
    end

    send_data = send_data .. type_data .. value_data

    return send_data
end

_M.construct_com_stmt_execute_packet = construct_com_stmt_execute_packet

local function parse_binary_resultset_row_packet(data, column_defs)
    local res, pos = {}, 1
    pos = pos + 1 --packet_header:[00]
    local num_columns = #column_defs
    local null_bitmap_len = math.modf((num_columns + 7 + 2) / 8)
    --print('null_bitmap_len=' .. null_bitmap_len)
    local null_bitmap = {}
    for i = 1, null_bitmap_len do
        null_bitmap[i], pos = conv.byte1_to_integer(data, pos)
    end
    for i = 1, num_columns do
        local byte_pos = math.modf((i - 1 + 2) / 8) + 1
        local bit_pos = (i - 1 + 2) % 8
        if band(null_bitmap[byte_pos], lshift(1, bit_pos)) > 0 then
            res[column_defs[i].name] = nil
        else
            res[column_defs[i].name], pos = conv.ProtocolBinaryConverters[column_defs[i].type][1](data, pos, column_defs[i].decimals)
        end
    end

    return res
end

_M.parse_binary_resultset_row_packet = parse_binary_resultset_row_packet

local function construct_com_stmt_close_packet(stmt)
    local send_data = conv.integer_to_byte1(COM_STMT_CLOSE)
    send_data = send_data .. conv.integer_to_byte4(stmt.statement_id)

    return send_data
end

_M.construct_com_stmt_close_packet = construct_com_stmt_close_packet

local function construct_com_stmt_reset_packet(stmt)
    local send_data = conv.integer_to_byte1(COM_STMT_RESET)
    send_data = send_data .. conv.integer_to_byte4(stmt.statement_id)

    return send_data
end

_M.construct_com_stmt_reset_packet = construct_com_stmt_reset_packet

local function compute_token(password, scramble)
    if password == "" then
        return ""
    end

    local stage1 = sha1.binary(password)
    local stage2 = sha1.binary(stage1)
    local stage3 = sha1.binary(scramble .. stage2)
    local n = #stage1
    local bytes = new_tab(n, 0)
    for i = 1, n do
         bytes[i] = strchar(bxor(strbyte(stage3, i), strbyte(stage1, i)))
    end

    return concat(bytes)
end

_M.compute_token = compute_token

return _M