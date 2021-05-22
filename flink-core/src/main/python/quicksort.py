'''
my first program after graduation from university
for a anniversary
'''

def find(L):
    t=L[0]
    count=0
    for i in range(1,len(L)):
        x=L[i]
        if L[i]<=t:
            a=L[0:i]
            L[1:i+1]=a
            L[0]=x
            count+=1
    return count

def quicksort(L):
    n=find(L)
    if n>=2:
        L[0:n]=quicksort(L[0:n])
    if len(L)-n>=2:
        L[n+1:len(L)]=quicksort(L[n+1:len(L)])
    return L
L = [877848,0,-1,-8,8,-5,784,8,1,20,155,10,9,6,4,16,5,13,26,18,2,45,34,23,1,7,3,88,77,4,2,6,3,9,8,9,6,487,74,74,7,12,45,78,4,52,88,77,14523]

print(quicksort(L))
