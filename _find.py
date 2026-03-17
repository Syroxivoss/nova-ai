lines = open('pyhton.py', encoding='utf-8').readlines()
keys = ['SimHashDedup', 'LongContext', 'BPE_FACTOR', 'build_long_chunk', 'for existing in self.fingerprints']
for i, l in enumerate(lines, 1):
    for k in keys:
        if k in l:
            print(f'{i}: {l.rstrip()[:100]}')
            break
