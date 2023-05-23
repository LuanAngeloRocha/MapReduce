#Contador de palavras com MaPReduce

def mapper(texto):
    text = texto.lower().strip()
    text = ''.join([c for c in text if c.isalpha() or c.isspace()])
    
    words = texto.split()
    
    word_counts = []
    for word in words:
        word_counts.append((word, 1))
    
    return word_counts

def reducer(word, counts):
    return word, sum(counts)

def map_reduce(texts):
    mapped_data = []
    for texto in texts:
        mapped_data.extend(mapper(texto))
    
    mapped_data.sort(key=lambda x: x[0])
    
    reduced_data = []
    i = 0
    while i < len(mapped_data):
        word = mapped_data[i][0]
        counts = [x[1] for x in mapped_data[i:i+2] if x[0] == word]
        reduced_data.append(reducer(word, counts))
        i += len(counts)
    
    return reduced_data

texts = [
    "Meu cachorro se chama Tom.",
    "A minha moto gasta muito .",
    "O meu cachorro mija no pneu da minha moto."
]
resultado = map_reduce(texts)
print(resultado)
