with open(r'c:\Users\Jay Mordaunt\Desktop\Clients\sparrow-erp-platinum\app\plugins\website_module\templates\public\index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    print(f'Total lines: {len(lines)}')
    for i in range(max(1, len(lines)-10), len(lines)+1):
        print(f'{i}: {lines[i-1].strip()}')
