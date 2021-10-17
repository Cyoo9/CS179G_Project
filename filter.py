import json
import re

f = open('pull_requests_copy.json')

data = json.load(f) 
output_file = open('pull_requests_filtered.json', 'w') 

for pr in data:
    if pr['linked_issue']:
        pr['linked_issue'][1] = pr['linked_issue'][1].replace('\n', '')
        pr['linked_issue'][0] = ''

f.close()

json.dump(data, output_file)

output_file.close()