import json
import re

f = open('pull_requests_copy.json')
f2 = open('issues_copy.json')

pull_requests = json.load(f) 
# will not put empty pull requests into the filtered JSON file, (basically if the id does not exist, remove it)
pull_requests = [pr for pr in pull_requests if pr['id']]

issues = json.load(f2)
output_file = open('pull_requests_filtered.json', 'w') 
output_file2 = open('issues_filtered.json', 'w') 

f.close()
f2.close()

for pr in pull_requests:
    if pr['linked_issue']:
        pr['linked_issue'][1] = pr['linked_issue'][1].replace('\n', '').replace(' ', '')
        pr['linked_issue'][0] = ''

for issue in issues:
    issue['title'] = issue['title'].replace(' ', '')

json.dump(pull_requests, output_file)
json.dump(issues, output_file2)

output_file.close()
output_file2.close()