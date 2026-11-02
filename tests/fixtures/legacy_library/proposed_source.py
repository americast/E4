def operator(inputs, params, runtime):
    candidates_env = contract_fields(inputs['candidates'], 'CandidateSet')
    logs_env = contract_fields(inputs['logs'], 'Aggregate')
    
    # Raw rows from bank.public.query are located in metadata.public_query.rows_data
    rows = logs_env['metadata']['public_query']['rows_data']
    pattern = params['pattern']
    
    scored_candidates = []
    
    for cand_env in candidates_env['candidates']:
        cand = contract_fields(cand_env, 'Candidate')
        entity_ref = contract_fields(cand['entity'], 'EntityRef')
        cid = entity_ref['identifier']
        
        count = 0
        for row in rows:
            # Row keys are native column names from log/log_service.csv
            if row.get('cmdb_id') == cid:
                val = row.get('value', '')
                if val and pattern in val:
                    count += 1
        
        if count > 0:
            evidence = {
                '$type': 'Evidence',
                '$fields': {
                    'contributing_entities': {'$tuple': [cand['entity']]},
                    'details': {'ranking_eligible': True},
                    'source': 'bank/log/log_service.csv',
                    'statistic': 'pattern_occurrence_count',
                    'support': count,
                    'value': float(count),
                    'window': logs_env['window']
                }
            }
            
            scored_candidates.append({
                '$type': 'ScoredCandidate',
                '$fields': {
                    'analysis_family': 'bank_log_content',
                    'category': None,
                    'entity': cand['entity'],
                    'evidence': {'$tuple': [evidence]},
                    'score': float(count)
                }
            })
            
    return {
        '$type': 'ScoredCandidates',
        '$fields': {
            'candidates': {'$tuple': scored_candidates},
            'scorer': 'evolved.log_pattern_scorer',
            'status': 'ready',
            'status_details': {}
        }
    }