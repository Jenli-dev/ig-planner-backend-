import sys, re

def main():
    path = sys.argv[1]
    start = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    lines = [ln.replace('\t', '    ') for ln in lines]

    block_re = re.compile(
        r'^\s*(?:async\s+def|def|class|if|elif|else:|for|while|try:|except\b|finally:|with|async\s+with)\b'
    )

    def is_block_opener(line: str) -> bool:
        code = line.split('#', 1)[0].rstrip()
        return code.endswith(':') and bool(block_re.match(code))

    i = start - 1
    n = len(lines)

    while i < n:
        if is_block_opener(lines[i]):
            base_indent = len(lines[i]) - len(lines[i].lstrip(' '))
            j = i + 1
            while j < n and (lines[j].strip() == '' or lines[j].lstrip().startswith('#')):
                j += 1
            if j < n:
                cur_indent = len(lines[j]) - len(lines[j].lstrip(' '))
                if cur_indent <= base_indent:
                    lines[j] = ' ' * (base_indent + 4) + lines[j].lstrip()
            i = j
        else:
            i += 1

    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(lines)

    print(f"Fixed indentation from line {start} → EOF.")

if __name__ == "__main__":
    main()
