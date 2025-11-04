
def print_title(title:str, symbol:str="#", lenght:int=70, skip:int=5):
    dec = symbol*lenght
    while True:        
        l = lenght-(skip*2)-len(title)
        # print(l)
        if l <=0:
            lenght += skip
            continue

        b = l//2       # blank
        # print(b)
        # print(l%2)
        if (l%2):
            start = skip*symbol + " " * b
            end = " " * (b + 1 ) + skip*symbol 
        else:
            start = end = skip*symbol + " " * b
            end = " " * b + skip*symbol 
        print("")
        print(lenght*symbol)
        print(start + title + end)
        print(lenght*symbol)
        break
 
if __name__ == '__main__':            
    print_title("Hello, world!")