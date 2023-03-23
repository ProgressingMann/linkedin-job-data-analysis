from db import *
import regex as re


def extract_programming_languages(job_id, jd):
    pls_d = {'python': 'Python', 'sql': 'SQL', 'java': 'Java', 'scala': 'Scala', 'matlab': 'MATLAB', 
            'julia': 'Julia', 'c\+\+': 'C++', 'javascript': 'JavaScript'}
    pls_found = []
    jd_lower = jd.lower()
    for pl in pls_d:
        if re.search(pl, jd_lower):
            pls_found.append((job_id, pls_d[pl]))
    
    spec_pls = {'[\W]R[\W]': 'R', 'SAS': 'SAS'}
    for pl in spec_pls:
        if re.search(pl, jd):
            pls_found.append((job_id, spec_pls[pl]))

    return pls_found