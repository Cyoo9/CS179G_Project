# CS179G_Project
- Facebook React analytics project using Spark

# After cloning
- git branch *your-branch-name*
- git checkout *your-branch-name*
- git push --set-upstream origin *your-branch-name*

# Installing dependencies
- In directory with requirements.txt, run command in terminal
1. pip install -r requirements.txt

# cs179_crawler 
- This directory has the 3 spiders that crawl issues, releases, and pull requests. 

# output files
- issues.json (github issues)
- releases.json (github releases)
- pull_requests.json (github pull requests)

# Run Django Server
- make sure you're in directory with manage.py
- run in terminal: python3 manage.py runserver
- open http://127.0.0.1:8000/ in web browser
