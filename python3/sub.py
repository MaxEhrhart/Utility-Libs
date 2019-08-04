def sub(a: str, b: str, virgula="."):
    tam_a = len(a.split(virgula)[1])
    tam_b = len(b.split(virgula)[1])

    if tam_a >= tam_b:
        pos_virgula = b.index(".")
        b += "0" * (tam_a - tam_b)
    else:
        pos_virgula = a.index(".")
        a += "0" * (tam_b - tam_a)

    a = a.replace(".", "")
    b = b.replace(".", "")

    print(a, b, pos_virgula, sep="\n")
    pos_virgula

    resultado = a[0:pos_virgula] + virgula + a[virgula:]
    print(resultado)
    resultado = resultado[0:pos_virgula] + virgula + resultado[virgula:]
    print(resultado)


a, b = str(1.123456095095), str(2.123)
print(sub(a, b))
