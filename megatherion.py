from abc import abstractmethod, ABC
from json import load
from numbers import Real
from pathlib import Path
from typing import Dict, Iterable, Iterator, Tuple, Union, Any, List, Callable
from enum import Enum
from collections.abc import MutableSequence


class Type(Enum):
    Float = 0
    String = 1


def to_float(obj) -> float:
    """
    casts object to float with support of None objects (None is cast to None)
    """
    return float(obj) if obj is not None else None


def to_str(obj) -> str: #huh
    """
    casts object to float with support of None objects (None is cast to None) 
    """
    return str(obj) if obj is not None else None


def common(iterable): # from ChatGPT
    """
    returns True if all items of iterable are the same.
    :param iterable:
    :return:
    """
    try:
        # Nejprve zkusíme získat první prvek iterátoru
        iterator = iter(iterable)
        first_value = next(iterator)
    except StopIteration:
        # Vyvolá výjimku, pokud je iterátor prázdný
        raise ValueError("Iterable is empty")

    # Kontrola, zda jsou všechny další prvky stejné jako první prvek
    for value in iterator:
        if value != first_value:
            raise ValueError("Not all values are the same")

    # Vrací hodnotu, pokud všechny prvky jsou stejné
    return first_value


class Column(MutableSequence):# implement MutableSequence (some method are mixed from abc)
    """
    Representation of column of dataframe. Column has datatype: float columns contains
    only floats and None values, string columns contains strings and None values.
    """
    def __init__(self, data: Iterable, dtype: Type):
        self.dtype = dtype
        self._cast = to_float if self.dtype == Type.Float else to_str
        # cast function (it casts to floats for Float datatype or
        # to strings for String datattype)
        self._data = [self._cast(value) for value in data]
    
    #TODO: Funkce která vrátí typ Columnu
    def get_type(self):
        return self.dtype

    def __len__(self) -> int:
        """
        Implementation of abstract base class `MutableSequence`.
        :return: number of rows
        """
        return len(self._data)

    def __getitem__(self, item: Union[int, slice]) -> Union[float,
                                    str, list[str], list[float]]:
        """
        Indexed getter (get value from index or sliced sublist for slice).
        Implementation of abstract base class `MutableSequence`.
        :param item: index or slice
        :return: item or list of items
        """
        return self._data[item]

    def __setitem__(self, key: Union[int, slice], value: Any) -> None:
        """
        Indexed setter (set value to index, or list to sliced column)
        Implementation of abstract base class `MutableSequence`.
        :param key: index or slice
        :param value: simple value or list of values
        """
        self._data[key] = self._cast(value)

    def append(self, item: Any) -> None:
        """
        Item is appended to column (value is cast to float or string if is not number).
        Implementation of abstract base class `MutableSequence`.
        :param item: appended value
        """
        self._data.append(self._cast(item))

    def insert(self, index: int, value: Any) -> None:
        """
        Item is inserted to colum at index `index` (value is cast to float or string if is not number).
        Implementation of abstract base class `MutableSequence`.
        :param index:  index of new item
        :param value:  inserted value
        :return:
        """
        self._data.insert(index, self._cast(value))

    def __delitem__(self, index: Union[int, slice]) -> None:
        """
        Remove item from index `index` or sublist defined by `slice`.
        :param index: index or slice
        """
        del self._data[index]

    def permute(self, indices: List[int]) -> 'Column':
        """
        Return new column which items are defined by list of indices (to original column).
        (eg. `Column(["a", "b", "c"]).permute([0,0,2])`
        returns  `Column(["a", "a", "c"])
        :param indices: list of indexes (ints between 0 and len(self) - 1)
        :return: new column
        """
        if len(indices) != len(self):
            raise Exception("TOO MANY/FEW iNDICES.")
        help = [self._data[i] for i in indices]
        #help = []
        #for i in indices:
        #    help.append(self.data[i])  
        

        return Column(help, self.dtype)
        #...

    def copy(self) -> 'Column':
        """
        Return shallow copy of column.
        :return: new column with the same items
        """
        # FIXME: value is cast to the same type (minor optimisation problem)
        return Column(self._data, self.dtype)

    def get_formatted_item(self, index: int, *, width: int):
        """
        Auxiliary method for formating column items to string with `width`
        characters. Numbers (floats) are right aligned and strings left aligned.
        Nones are formatted as aligned "n/a".
        :param index: index of item
        :param width:  width
        :return:
        """
        assert width > 0
        if self._data[index] is None:
            if self.dtype == Type.Float:
                return "n/a".rjust(width)
            else:
                return "n/a".ljust(width)
        return format(self._data[index],
                      f"{width}s" if self.dtype == Type.String else f"-{width}.2g")

class DataFrame:
    """
    Dataframe with typed and named columns
    """
    def __init__(self, columns: Dict[str, Column]):
        """
        :param columns: columns of dataframe (key: name of dataframe),
                        lengths of all columns has to be the same
        """
        assert  len(columns) > 0, "Dataframe without columns is not supported"
        self._size = common(len(column) for column in columns.values())
        # deep copy od dict `columns`
        self._columns = {name: column.copy() for name, column in columns.items()}

    def __getitem__(self, index: int) -> Tuple[Union[str,float]]:
        """
        Indexed getter returns row of dataframe as tuple
        :param index: index of row
        :return: tuple of items in row
        """
        if index >= len(self):
            raise IndexError("Value of index is too large")
        if index < 0:
            raise IndexError("Value of index is too low")
        
        #return tuple(column[index] for column in self._columns.values()) 
        help = []
        for column in self._columns.values():
            help.append(column[index])
        return tuple(help)
        #...

    def __iter__(self) -> Iterator[Tuple[Union[str, float]]]:
        """
        :return: iterator over rows of dataframe
        """
        for i in range(len(self)):
            yield tuple(c[i] for c in self._columns.values())

    def __len__(self) -> int:
        """
        :return: count of rows
        """
        return self._size

    @property
    def columns(self) -> Iterable[str]:
        """
        :return: names of columns (as iterable object)
        """
        return self._columns.keys()

    def __repr__(self) -> str:
        """
        :return: string representation of dataframe (table with aligned columns)
        """
        lines = []
        lines.append(" ".join(f"{name:12s}" for name in self.columns))
        for i in range(len(self)):
            lines.append(" ".join(self._columns[cname].get_formatted_item(i, width=12)
                                     for cname in self.columns))
        return "\n".join(lines)

    def append_column(self, col_name:str, column: Column) -> None:
        """
        Appends new column to dataframe (its name has to be unique).
        :param col_name:  name of new column
        :param column: data of new column
        """
        if col_name in self._columns:
            raise ValueError("Duplicate column name")
        self._columns[col_name] = column.copy()
        ...    

    def append_row(self, row: Iterable) -> None:
        """
        Appends new row to dataframe.
        :param row: tuple of values for all columns
        """
        print(row)
        row = list(row) #I hate tuples
        print(row)
        if len(row) != len(self._columns):
             raise ValueError("Row has more values than there are columns") #TODO: dej tam variabli at se lip debuguje
        
        col_keys = list(self._columns.keys())
        print(col_keys)

        for i in range(len(self._columns)):
            col_name = col_keys[i]
            value = row[i]

            col = self._columns[col_name]

            col.append(value)
        self._size += 1
        #TODO: zkontrolovat aby row mel delku stejnou jako pocet columns, pripadne pridat NaN value; zkontrolovat datatype
        


    def filter(self, col_name:str,
               predicate: Callable[[Union[float, str]], bool]) -> 'DataFrame':
        """
        Returns new dataframe with rows which values in column `col_name` returns
        True in function `predicate`.

        :param col_name: name of tested column
        :param predicate: testing function
        :return: new dataframe
        """

        ...

    def sort(self, col_name:str, ascending=True) -> 'DataFrame':
        """
        Sort dataframe by column with `col_name` ascending or descending.
        :param col_name: name of key column
        :param ascending: direction of sorting
        :return: new dataframe
        """
        ...

    def describe(self) -> str:
        """
        similar to pandas but only with min, max and avg statistics for floats and count"
        :return: string with formatted decription
        """
        ...

    def inner_join(self, other: 'DataFrame', self_key_column: str,
                   other_key_column: str) -> 'DataFrame':
        """
            Inner join between self and other dataframe with join predicate
            `self.key_column == other.key_column`.

            Possible collision of column identifiers is resolved by prefixing `_other` to
            columns from `other` data table.
        """
        ...

    def setvalue(self, col_name: str, row_index: int, value: Any) -> None:
        """
        Set new value in dataframe.
        :param col_name:  name of culumns
        :param row_index: index of row
        :param value:  new value (value is cast to type of column)
        :return:
        """
        col = self._columns[col_name]
        col[row_index] = col._cast(value)
    
    def column_type(self, col_name):
        assert isinstance(col_name, str), "should be string"
        return self._columns[col_name].get_type()


    def transpose(self):
           df_len = len(self)
           print(self[0])
           col_type = Type.Float
           for key in self.columns:
               self.column_type(key)
               if self.column_type(key) == Type.String:
                   col_type = Type.String
                   break
                   
           df_t = DataFrame({
               'col0' : Column(self[0], col_type)
               }
           )
           for i in range(1, len(self)):
               df_t.append_column(
                   "col"+str(i),
                    Column(self[i], col_type) )
            

            #TODO: Zkontrolovat jestli má nějaké column string a v případě změnit typ :DONE

           return df_t

    def product(self, axis: int = 0):
        product = []
        if axis == 1:
            for row in range(0,len(self)):
                #print("we are on row" +str(row))
                multi = 1
                for value in self[row]:
                    #print("culum")
                    multi *= value
                    print(multi)
                    #print(i)
                product.append(multi)    
                
        elif axis == 0:
            for column in self._columns.values():
                #print("culum")
                multi = 1
                for value in column._data:
                    multi *= value
                    #print(multi)
                product.append(multi)
        
        print(product)
        pf = DataFrame({'product': Column(product, Type.Float)})
        return pf
        #TODO:osetrit string typy 

    def replace(self, to_replace, value):
        if type(to_replace) != list:
            to_replace = [to_replace]
            #print(to_replace)
            
        for column in self._columns.values():
            for i in range(len(column)):
                if column[i] in to_replace:
                    column[i] = value
                print(column[i])
    
    def shift(self, periods):
        nd = {}
        for key in self.columns:
            sl = []
            for n in range(periods):
                sl.append("NaN")
            column = self._columns[key]
            for i in range(len(column)-periods):
                sl.append(column[i])
            print(sl)
            nd[key] = Column(sl, Type.Float)

        ndf = DataFrame(nd)
        #print(ndf)
        return(ndf)
            #TODO: udelej dictionary, key je key, values jsou list sl[], nezapomenout na type(pozdeji)

    def cumprod(self, axis: int = 0):
        key_list = []
        k=-1
        for key in self._columns.keys():
            key_list.append(key)
        if axis == 0:
            df_p = DataFrame({ 'delete' : Column([11], Type.Float) })
            for column in self._columns.values(): #DO EVERYTHING, THROUGH THE FUCKING KEYS YOU PINAPPLE-SPRINKLED-DONUT, THIS DOESNT WORK!!!!!
                k += 1
                dl = []
                m = 1
                print('new column')
                print(column._data)
                for i in range(len(column)):
                    value = column[i]
                    tm = column[i]
                    value *= m
                    m *= tm
                    print(column[i])
                    print(m)
                    dl.append(value)
                    
                    #TODO: osetrit kdyz int neni 1 nebo dva, osertrit string typy
                    
                df_p.append_column(key_list[k], Column(dl, Type.Float))
                print(df_p)

            df_p._columns.pop('delete')
            return df_p
                

        else:
            df_p = DataFrame({ 'delete' : Column([], Type.Float) })
            
            for i in range(len(self)):
                k += 1
                m = 1
                print('new row')
                for value in self[i]:
                    tm = value
                    f_value = value
                    f_value *= m
                    m *= tm
                    dl.append(f_value)
                
                
        



    

# if __name__ == "__main__":
#    df = DataFrame(dict(
#        a=Column([None, 3.1415], Type.Float),
#        b=Column(["a", 2], Type.String),
#        c=Column(range(2), Type.Float)
#        ))
#    df.setvalue("a", 1, 42)
#    print(df)

# for line in df:
#    print(line)

#print(df[1][1])

#print(df.transpose())

#test = list((7,7,7,7))
#print(test)
#test2 = [7,7,7,2]
#print(test2)
kus_dreva = DataFrame(dict(
     a=Column([7, 5, 6], Type.Float),
     b=Column([1, 2.3, 0.5], Type.Float),
     ))
#print("mezera")
#print(kus_dreva.__getitem__(1))
#print(kus_dreva.column_type("f"))
#print("mezera")
#kus_dreva.append_column("c", Column(["o","cholera"], Type.String)) 
kus_dreva.append_column("c", Column([5, 47, 4], Type.Float))
#print(kus_dreva)
#print(kus_dreva.transpose())
#print(kus_dreva.product(1))
#kus_dreva._columns['a'][0] = 1 
#print(kus_dreva._columns['a'][0])

#kus_dreva.replace([7,5], 13)
#print(kus_dreva)
#kus_dreva.append_row((9,8,6))
#print(kus_dreva)
#kus_dreva.shift(1)

print(kus_dreva.cumprod(0))

