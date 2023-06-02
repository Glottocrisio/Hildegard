file = ""

metadata  = []

lines = 100000    # 100k for testing

with open(file, 'r') as f:
    
    for line in tqdm(f):
        metadata.append(json.loads(line))
        lines -= 1
        if lines == 0: break
            
df = pd.DataFrame(metadata)
