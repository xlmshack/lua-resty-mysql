local bit = require "bit"
local sub = string.sub
local tcp = ngx.socket.tcp
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local format = string.format
local strrep = string.rep
local null = ngx.null
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex
local sha1 = ngx.sha1_bin
local concat = table.concat
local unpack = unpack
local setmetatable = setmetatable
local error = error
local tonumber = tonumber

local _M

local function byte1_to_integer(data, index)
    local a = strbyte(data, index, index)
    return a, index + 1
end

_M.byte1_to_integer = byte1_to_integer

local function byte2_to_integer(data, index)
    local a, b = strbyte(data, index, index + 1)
    return bor(a, lshift(b, 8)), index + 2
end

_M.byte2_to_integer = byte2_to_integer

local function byte3_to_integer(data, index)
    local a, b, c = strbyte(data, index, index + 2)
    return bor(a, lshift(b, 8), lshift(c, 16)), index + 3
end

local _M.byte3_to_integer = byte3_to_integer

local function byte4_to_integer(data, index)
    local a, b, c, d = strbyte(data, index, index + 3)
    return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24)), index + 4
end

_M.byte4_to_integer = byte4_to_integer

local function byte8_to_integer(data, index)
    local a, b, c, d, e, f, g, h = strbyte(data, index, index + 7)

    -- XXX workaround for the lack of 64-bit support in bitop:
    local lo = bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24))
    local hi = bor(e, lshift(f, 8), lshift(g, 16), lshift(h, 24))
    return lo + hi * 0x100000000, index + 8
end

_M.byte8_to_integer = byte8_to_integer

local function integer_to_byte1(n)
    return strchar(band(n, 0xff))
end

_M.integer_to_byte1 = integer_to_byte1

local function integer_to_byte2(n)
    return strchar(band(n, 0xff), band(rshift(n, 8), 0xff))
end

_M.integer_to_byte2 = integer_to_byte2

local function integer_to_byte3(n)
    return strchar(band(n, 0xff),
                   band(rshift(n, 8), 0xff),
                   band(rshift(n, 16), 0xff))
end

_M.integer_to_byte3 = integer_to_byte3

local function integer_to_byte4(n)
    return strchar(band(n, 0xff),
                   band(rshift(n, 8), 0xff),
                   band(rshift(n, 16), 0xff),
                   band(rshift(n, 24), 0xff))
end

_M.integer_to_byte4 = integer_to_byte4

local function integer_to_byte8(n)
    return strchar(band(n, 0xff),
                   band(rshift(n, 8), 0xff),
                   band(rshift(n, 16), 0xff),
                   band(rshift(n, 24), 0xff),
                   band(rshift(n, 32), 0xff),
                   band(rshift(n, 40), 0xff),
                   band(rshift(n, 48), 0xff),
                   band(rshift(n, 56), 0xff))
end

_M.integer_to_byte8 = integer_to_byte8

function _M.cstr_to_str(data, index)
    local last = strfind(data, "\0", index, true)
    if not last then
        return nil, nil
    end

    return sub(data, index, last), last + 1
end

function _M.str_to_cstr(data)
    return data .. "\0"
end

local function lenenc_to_integer(data, index)
    local first, new_index = byte1_to_integer(data, index)
    if not first then
        return nil, index
    end

    if first >= 0 and first < 0xfb then
        return first, new_index
    elseif first == 0xfb then --EOF
        return null, new_index
    elseif first == 0xfc then --2-byte integer
        return byte2_to_integer(data, new_index)
    elseif first == 0xfd then --3-byte integer
        return byte3_to_integer(data, new_index)
    elseif first == 254 then --8-byte integer
        return byte8_to_integer(data, new_index)
    else --ERR
        return false, new_index
    end
end

local function integer_to_lenenc(n)
    if n >=0 and n < 0xfb then
        return integer_to_byte1(n)
    elseif n >= 0xfb and n < 0x10000 then --2^16
        return integer_to_byte1(0xfc) .. integer_to_byte2(n)
    elseif n >= 0x10000 and n < 0x1000000 then --2^24
        return integer_to_byte1(0xfd) .. integer_to_byte3(n)
    else--if n >= 0x1000000 and n < 0x10000000000000000 then --2^64
        return integer_to_byte1(0xfe) .. integer_to_byte8(n)
end

local function lenenc_str_to_str(data, index)
    local data_length, new_index = lenenc_to_integer(data, index)
    if data_length == nil or data_length == null then
        return null, new_index
    end

    return sub(data, new_index, new_index + data_length - 1), new_index + data_length
end

_M.lenenc_str_to_str = lenenc_str_to_str

local function str_to_lenenc_str(data)
    return integer_to_lenenc(#data) .. data
end

_M.str_to_lenenc_str = str_to_lenenc_str

local function float_to_byte4(n)
    if n == 0 then
        return string.char(0x00, 0x00, 0x00, 0x00)
    elseif n ~= n then
        return string.char(0xFF, 0xFF, 0xFF, 0xFF)
    else
        local sign = 0x00
        if n < 0 then
            sign = 0x80
            n = -n
        end
        local mantissa, exponent = math.frexp(n)
        exponent = exponent + 0x7F
        if exponent <= 0 then
            mantissa = math.ldexp(mantissa, exponent - 1)
            exponent = 0
        elseif exponent > 0 then
            if exponent >= 0xFF then
                return string.char(sign + 0x7F, 0x80, 0x00, 0x00)
            elseif exponent == 1 then
                exponent = 0
            else
                mantissa = mantissa * 2 - 1
                exponent = exponent - 1
            end
        end
        mantissa = math.floor(math.ldexp(mantissa, 23) + 0.5)
        return string.char(
                sign + math.floor(exponent / 2),
                (exponent % 2) * 0x80 + math.floor(mantissa / 0x10000),
                math.floor(mantissa / 0x100) % 0x100,
                mantissa % 0x100)
    end
end

local function byte4_to_float(data, index)
    local b1, b2, b3, b4 = string.byte(data, index, index + 3)
    local exponent = (b1 % 0x80) * 0x02 + math.floor(b2 / 0x80)
    local mantissa = math.ldexp(((b2 % 0x80) * 0x100 + b3) * 0x100 + b4, -23)
    if exponent == 0xFF then
        if mantissa > 0 then
            return 0 / 0, index + 4
        else
            mantissa = math.huge
            exponent = 0x7F
        end
    elseif exponent > 0 then
        mantissa = mantissa + 1
    else
        exponent = exponent + 1
    end
    if b1 >= 0x80 then
        mantissa = -mantissa
    end
    return math.ldexp(mantissa, exponent - 0x7F), index + 4
end

local function double_to_byte8(n)
    return nil
end

local function byte8_to_double(data)
    return nil
end

local function datetime_to_str(data, index)
    local data_length, new_index = byte1_to_integer(data, index)
    if data_length == 0 then
        return '0000-00-00 00:00:00', new_index
    elseif data_length == 4 then
        local _year, new_index = byte2_to_integer(data, new_index)
        local _month, new_index = byte1_to_integer(data, new_index)
        local _day, new_index = byte1_to_integer(data, new_index)
        local datetime_str = format('%4d-%02d-%02d', _year, _month, _day)
        return datetime_str, new_index
    elseif data_length == 7 then
        local _year, new_index = byte2_to_integer(data, new_index)
        local _month, new_index = byte1_to_integer(data, new_index)
        local _day, new_index = byte1_to_integer(data, new_index)
        local _hour, new_index = byte1_to_integer(data, new_index)
        local _minute, new_index = byte1_to_integer(data, new_index)
        local _second, new_index = byte1_to_integer(data, new_index)
        local datetime_str = format('%4d-%02d-%02d %02d:%02d:%02d', _year, _month, _day, _hour, _minute, _second)
        return datetime_str, new_index
    elseif data_length == 11 then
        local _year, new_index = byte2_to_integer(data, new_index)
        local _month, new_index = byte1_to_integer(data, new_index)
        local _day, new_index = byte1_to_integer(data, new_index)
        local _hour, new_index = byte1_to_integer(data, new_index)
        local _minute, new_index = byte1_to_integer(data, new_index)
        local _second, new_index = byte1_to_integer(data, new_index)
        local _microsec, new_index = byte4_to_integer(data, new_index)
        local datetime_str = format('%4d-%02d-%02d %02d:%02d:%02d', _year, _month, _day, _hour, _minute, _second)
        return datetime_str, new_index
    else
        return nil
    end
end

local function str_to_datetime(data)
    local data_length = #data
    if data == '0000-00-00 00:00:00' then
        return integer_to_byte1(0)
    end

    if data_length == #'2010-10-17 19:27:30' then
        local _year = tonumber(sub(data, 1, 4))
        local _month = tonumber(sub(data, 6, 7))
        local _day = tonumber(sub(data, 9, 10))
        local _hour = tonumber(sub(data, 12, 13))
        local _minute = tonumber(sub(data, 15, 16))
        local _second = tonumber(sub(data, 18, 19))
        return integer_to_byte1(7)
            .. integer_to_byte2(_year)
            .. integer_to_byte1(_month)
            .. integer_to_byte1(_day)
            .. integer_to_byte1(_hour)
            .. integer_to_byte1(_minute)
            .. integer_to_byte1(_second)
    elseif data_length == #'2010-10-17' then
        local _year = tonumber(sub(data, 1, 4))
        local _month = tonumber(sub(data, 6, 7))
        local _day = tonumber(sub(data, 9, 10))
        return integer_to_byte1(4)
            .. integer_to_byte2(_year)
            .. integer_to_byte1(_month)
            .. integer_to_byte1(_day)
    else
        return nil
    end
end

local function time_to_str(data, index)
    local data_length, new_index = byte1_to_integer(data, index)
    if data_length == 0 then
        return '0d 00:00:00', new_index
    elseif data_length == 8 then
        local _is_negative, new_index = byte1_to_integer(data, new_index)
        local _day, new_index = byte4_to_integer(data, new_index)
        local _hour, new_index = byte1_to_integer(data, new_index)
        local _minute, new_index = byte1_to_integer(data, new_index)
        local _second, new_index = byte1_to_integer(data, new_index)
        local time_str = format('%02d:%02d:%02d', _hour, _minute, _second)
        return time_str, new_index
    elseif data_length == 12 then
        local _is_negative, new_index = byte1_to_integer(data, new_index)
        local _day, new_index = byte4_to_integer(data, new_index)
        local _hour, new_index = byte1_to_integer(data, new_index)
        local _minute, new_index = byte1_to_integer(data, new_index)
        local _second, new_index = byte1_to_integer(data, new_index)
        local _microsec, new_index = byte4_to_integer(data, new_index)
        local time_str = format('%02d:%02d:%02d', _hour, _minute, _second)
        return time_str, new_index
    else
        return nil
    end
end

local function str_to_time(data)
    local data_length = #data
    if data == '00:00:00' then
        return integer_to_byte1(0)
    elseif data_length == #'00:00:00' then
        local _is_negative = 0
        local _day = 0
        local _hour = tonumber(sub(data, 1, 2))
        local _minute = tonumber(sub(data, 4, 5))
        local _second = tonumber(sub(data, 7, 8))

        return integer_to_byte1(8)
            .. integer_to_byte1(_is_negative)
            .. integer_to_byte4(_day)
            .. integer_to_byte1(_hour)
            .. integer_to_byte1(_minute)
            .. integer_to_byte1(_second)
    else
        return nil
    end
end


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

local ProtocolTextConverters = new_tab(0, 8)
for i = 0x01, 0x05 do
    -- tiny, short, long, float, double
    ProtocolTextConverters[i] = tonumber
end
-- converters[0x08] = tonumber  -- long long
ProtocolTextConverters[0x09] = tonumber  -- int24
ProtocolTextConverters[0x0d] = tonumber  -- year
ProtocolTextConverters[0xf6] = tonumber  -- newdecimal

_M.ProtocolTextConverters = ProtocolTextConverters

local ProtocolBinaryConverters = new_tab(2, 30)
--database to native
ProtocolBinaryConverters[MYSQL_TYPE_DECIMAL][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_TINY][1] = byte1_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_SHORT][1] = byte2_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_LONG][1] = byte4_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_FLOAT][1] = byte4_to_float
ProtocolBinaryConverters[MYSQL_TYPE_DOUBLE][1] = nil
ProtocolBinaryConverters[MYSQL_TYPE_NULL][1] = nil
ProtocolBinaryConverters[MYSQL_TYPE_TIMESTAMP][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_LONGLONG][1] = byte8_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_INT24][1] = byte4_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_DATE][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_TIME][1] = time_to_str
ProtocolBinaryConverters[MYSQL_TYPE_DATETIME][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_YEAR][1] = byte2_to_integer
ProtocolBinaryConverters[MYSQL_TYPE_NEWDATE][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_VARCHAR][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_BIT][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_TIMESTAMP2][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_DATETIME2][1] = datetime_to_str
ProtocolBinaryConverters[MYSQL_TYPE_TIME2][1] = time_to_str
ProtocolBinaryConverters[MYSQL_TYPE_NEWDECIMAL][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_ENUM][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_SET][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_TINY_BLOB][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_MEDIUM_BLOB][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_LONG_BLOB][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_BLOB][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_VAR_STRING][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_STRING][1] = lenenc_str_to_str
ProtocolBinaryConverters[MYSQL_TYPE_GEOMETRY][1] = lenenc_str_to_str

--native to database
ProtocolBinaryConverters[MYSQL_TYPE_DECIMAL][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_TINY][2] = integer_to_byte1
ProtocolBinaryConverters[MYSQL_TYPE_SHORT][2] = integer_to_byte2
ProtocolBinaryConverters[MYSQL_TYPE_LONG][2] = integer_to_byte4
ProtocolBinaryConverters[MYSQL_TYPE_FLOAT][2] = float_to_byte4
ProtocolBinaryConverters[MYSQL_TYPE_DOUBLE][2] = nil
ProtocolBinaryConverters[MYSQL_TYPE_NULL][2] = nil
ProtocolBinaryConverters[MYSQL_TYPE_TIMESTAMP][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_LONGLONG][2] = integer_to_byte8
ProtocolBinaryConverters[MYSQL_TYPE_INT24][2] = integer_to_byte4
ProtocolBinaryConverters[MYSQL_TYPE_DATE][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_TIME][2] = str_to_time
ProtocolBinaryConverters[MYSQL_TYPE_DATETIME][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_YEAR][2] = integer_to_byte2
ProtocolBinaryConverters[MYSQL_TYPE_NEWDATE][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_VARCHAR][2] =str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_BIT][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_TIMESTAMP2][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_DATETIME2][2] = str_to_datetime
ProtocolBinaryConverters[MYSQL_TYPE_TIME2][2] = str_to_time
ProtocolBinaryConverters[MYSQL_TYPE_NEWDECIMAL][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_ENUM][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_SET][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_TINY_BLOB][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_MEDIUM_BLOB][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_LONG_BLOB][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_BLOB][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_VAR_STRING][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_STRING][2] = str_to_lenenc_str
ProtocolBinaryConverters[MYSQL_TYPE_GEOMETRY][2] = str_to_lenenc_str

_M.ProtocolBinaryConverters = ProtocolBinaryConverters

return _M