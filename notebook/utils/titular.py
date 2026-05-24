def imprimir_titulo(texto, nivel=1, ancho=50, caracter="="):
    """
    Imprime un título centrado con caracteres de decoración.
    
    :param texto: El texto que quieres mostrar.
    :param nivel: Si es 1 (título principal) o 2 (subtítulo).
    :param ancho: Longitud total de la línea.
    :param caracter: Caracter para los bordes.
    """
    linea = caracter * ancho
    print(f"\n{linea}")
    
    if nivel == 1:
        # Título centrado
        print(f"{texto.center(ancho, ' ')}")
    else:
        # Subtítulo (ej. Iteración X/Y)
        print(f"{texto:^{ancho}}")
        
    print(f"{linea}")
