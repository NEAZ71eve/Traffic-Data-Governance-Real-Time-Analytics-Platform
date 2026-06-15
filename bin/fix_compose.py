#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('docker-compose-production.yml', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Based on reading the file content, we know the exact line numbers and content
# Let's make targeted changes by line matching
new_lines = []
i = 0
while i < len(lines):
    line = lines[i]

    # 1. hive-metastore-db: comment out the ports lines
    if line.strip() == '# hive-metastore-db':
        new_lines.append(line)
        i += 1
        while i < len(lines) and not lines[i].strip().startswith('#'):
            new_lines.append(lines[i])
            i += 1
        continue

    # 2. hive-metastore volumes: add JDBC driver and core-site.xml
    if './sql:/opt/hive/sql:ro' in line:
        new_lines.append(line)
        new_lines.append(lines[i].rsplit('./sql', 1)[0].rstrip() + '      - ./postgresql-42.7.1.jar:/opt/hive/lib/postgresql-42.7.1.jar:ro\n')
        new_lines.append(lines[i].rsplit('./sql', 1)[0].rstrip() + '      - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro\n')
        i += 1
        continue

    # 3. hiveserver2: add volumes before command
    if 'depends_on:' in line and i+1 < len(lines) and '- hive-metastore' in lines[i+1]:
        new_lines.append(line)  # depends_on:
        i += 1
        new_lines.append(lines[i])  # - hive-metastore
        i += 1
        # Insert volumes section here
        indent = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
        new_lines.append(indent + 'volumes:\n')
        new_lines.append(indent + '  - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro\n')
        continue

    # 4. Comment out ports lines for hive-metastore-db
    if line.strip() == 'ports:' and i > 0 and 'hive-metastore-db' in lines[i-5].strip() if i >= 5 else '':
        new_lines.append('    # ports:\n')
        i += 1
        if i < len(lines) and '5432' in lines[i]:
            new_lines.append('    #   - "5432:5432"\n')
            i += 1
        continue

    new_lines.append(line)
    i += 1

with open('docker-compose-production.yml', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('Done! Verifying YAML...')
import yaml
with open('docker-compose-production.yml', 'r', encoding='utf-8') as f:
    data = yaml.safe_load(f)
print(f'YAML valid. Services: {list(data.get("services", {}).keys())}')
