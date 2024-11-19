def s():
    print('study yield')
    m = yield 5
    print(m)
    d = yield 16
    print('go on!')


c = s()
s_d = next(c)  # 相当于send(None)
print(s_d)
s_d = next(c)  # 相当于send(None)
# c.send('Fighting!')  # (yield 5)表达式被赋予了'Fighting!'