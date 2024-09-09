from megatherion import Column, DataFrame, Type

# test = [1, 2, 3, 4, 5, 6]
# to_replace = [3]
# value = "test"
# for i in range(len(test)):
#     if test[i] in to_replace:
#         test[i] = value
# print(test) 
# print(3 == "3")

sexrex = Column([1,2], Type.Float)
sexrex.append(3)
print(sexrex._data)

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