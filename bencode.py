
def bencode(obj):
    if isinstance(obj, str):
        return ''.join([str(len(obj)), ':', obj])
    elif isinstance(obj, unicode):
        return bencode(obj.encode())
    elif isinstance(obj, (int, long)):
        return ''.join(['i', str(obj), 'e'])
    elif isinstance(obj, list):
        content = [bencode(i) for i in obj]
        return ''.join(['l', ''.join(content), 'e'])
    elif isinstance(obj, dict):
        content = []
        for i in sorted(obj.keys()):
            content.append(bencode(i))
            content.append(bencode(obj[i]))
        return ''.join(['d', ''.join(content), 'e'])
    else:
        raise TypeError, 'unable to bencode %s' % type(obj)
    
def bdecode(s):
    try:
        ret, length = _bdecode(s, 0)
    except (IndexError, OverflowError, ValueError):
        raise ValueError, 'bad bencoded data'
    if length != len(s):
        raise ValueError, 'bad bencoded data'
    return ret

def _bdecode(s, i):
    if not s:
        raise ValueError
    if s[i].isdigit():
        return _decode_str(s, i)
    elif s[i] == 'i':
        return _decode_int(s, i)
    elif s[i] == 'l':
        return _decode_list(s, i)
    elif s[i] == 'd':
        return _decode_dict(s, i)
    else:
        raise ValueError
    
def _decode_str(s, i):
    colon = s.index(':', i)
    length = int(s[i:colon])
    if s[i] == '0' and i + 1 != colon:
        raise ValueError
    colon += 1
    ret = s[colon:colon+length]
    try:
        ret = unicode(ret, 'utf-8')
    except UnicodeError:
        pass
    return ret, colon + length

def _decode_int(s, i):
    i += 1
    end = s.index('e', i)
    if s[i] == '0' and i + 1 != end:
        raise ValueError
    elif s[i:i+2] == '-0':
        raise ValueError
    return int(s[i:end]), end + 1
    
def _decode_list(s, i):
    i += 1
    lst = []
    while s[i] != 'e':
        item, i = _bdecode(s, i)
        lst.append(item)
    return lst, i + 1
        
def _decode_dict(s, i):
    i += 1
    dct = {}
    while s[i] != 'e':
        key, i = _bdecode(s, i)
        value, i = _bdecode(s, i)
        dct[key] = value
    return dct, i + 1
