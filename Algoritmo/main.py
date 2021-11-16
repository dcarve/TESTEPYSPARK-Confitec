from random import randint #não foi especificado detalhes sobre que tipos de números aleatórios
#
#matriz_A = [[1,2,3,4],
#            [5,6,7,8],
#            [1,2,3,4],
#            [5,6,7,8]]
#
#matriz_B = [[1,2,3,4],
#            [5,6,7,8],
#            [1,2,3,4],
#            [5,6,7,8]]
#

matriz_A = [[randint(-9999,9999) for j in range(4)] for i in range(4)]
matriz_B = [[randint(-9999,9999) for j in range(4)] for i in range(4)]


def produto_matriz(matriz_A, matriz_B):
    return [[sum([matriz_A[i][n] * matriz_B[n][j] for n in range(4)]) for j in range(4)] for i in range(4)]
        
        
produto = produto_matriz(matriz_A, matriz_B)


print('matriz A:')
print(matriz_A)
print('matriz B:')
print(matriz_B)
print('produto:')
print(produto)
