from typing import Iterable,Optional,Any
from pathlib import Path
from importlib import import_module
from collections import namedtuple,defaultdict
from datetime import datetime



_FM = namedtuple("FILEMODE","jsonlib read write header_sep empty_sep")
_DTF = "%Y-%m-%dT%H:%M:%S.%f"
STD_DESERIALIZERS = {'datetime':lambda x : datetime.strptime(x,_DTF)}
STD_SERIALIZERS = {'datetime':lambda x : datetime.strftime(x,_DTF)}


def dump(data:Iterable[dict[str|int,str|int|float|bool|None]],path:Path|str,keys:Optional[list[str|int]] = None,types:Optional[dict[str,str]] = None,orjson:bool=True,key_disc:int=0)->None:
    if orjson:
        file_mode = _FM('orjson','rb','wb',b'\n',b'')        
    else:
        file_mode = _FM('json','r','w','\n','')
    jl = import_module(file_mode.jsonlib)
    contents,values = {},[]
    offset = 0
    if not types:
        types = {}
    if not keys:
        if key_disc==0:
            keys = list(data[0].keys())

        elif key_disc == 1:
            ak = []
            for obj in data:
                ak.extend(list(obj.keys()))
            keys = list(set(ak))
            for obj in data:
                for key in keys:
                    if key not in obj:
                        obj[key] = None

        elif key_disc == 2:
            keys = []
            key_count = defaultdict(int)
            for obj in data:
                for key in obj.keys():
                    key_count[key]+=1
            data_length = len(data)
            for key,occurance in key_count.items():
                if occurance == data_length:
                    keys.append(key)

        else:
            raise ValueError(key_disc)
    else:
        try:
            keys = [k for k in keys if k in list(data[0].keys())]
        except KeyError as ke:
            raise KeyError(f"Key {ke} does not exist in the data")
    for key in keys: 
        obj_values = [obj[key] for obj in data]
        serializer = types.get(key)
        serializer_str = None
        if serializer:         
            if isinstance(serializer,str):
                try:
                    serializer = STD_SERIALIZERS[serializer]
                except KeyError as ke:
                    raise KeyError(f"{ke} - Not a storable serializer")
                else:
                    serializer_str = str(serializer)
            obj_values = [serializer(v) for v in obj_values]

        obj_values_str = jl.dumps({'d':obj_values})
        offset_shift = len(obj_values_str)
        key_offset = (offset,offset+offset_shift)
        offset += offset_shift
        contents[key] = {'offset':key_offset,'type':serializer_str}
        values.append(obj_values_str)
    contents_str = jl.dumps(contents)
    value_str = file_mode.empty_sep.join(values)
    data_str = file_mode.header_sep.join((contents_str,value_str))

    with open(path,file_mode.write) as f:
        f.write(data_str)

def load(path:Path|str,keys:Optional[list[str|int]] = None,types:Optional[dict[str,str]] = None,orjson:bool=True)->list[dict[str|int,str|int|float|bool|None]]:
    if orjson:
        file_mode = _FM('orjson','rb','wb',b'\n',b'')        
    else:
        file_mode = _FM('json','r','w','\n','')
    jl = import_module(file_mode.jsonlib)
    if not types:
        types = {}
    with open(path,file_mode.read) as f:
        path_contents = jl.loads(f.readline())
        data_start = f.tell()
        if not keys:
            keys = list(path_contents.keys())
        values=[]
        objects = 0
        for key in keys:
            try:
                d_start,d_end = path_contents[key]['offset']
            except KeyError as e:
                raise KeyError(f"File does not contain the column {e}")
            else:
                d_start += data_start
                d_end += data_start
                f.seek(d_start)
                value_str = f.read(d_end-d_start)
                key_values = jl.loads(value_str)['d']
                deserializer = types.get(key)
                stored_deserializer = path_contents[key].get('type')
                if deserializer:
                    key_values = [deserializer(v) for v in key_values]
                elif stored_deserializer:
                    deserializer = STD_DESERIALIZERS[stored_deserializer]
                    key_values = [deserializer(v) for v in key_values]
                values.append(key_values)
                if not objects:
                    objects = len(key_values)
        return [{k:values[ki][oi] for (ki,k) in enumerate(keys)} for oi in range(objects)]
