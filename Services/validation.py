ALLOWED_RESOURCES = ['desktop', 'server']


def resource_validation(name: str, type: str):
    if not name or not type:
        return 'Invalid request - both name and type are required'
    if len(name) > 255:
        return f'Invalid name - max allowed length is 255 chars - requested {len(name)}'
    if type.lower() not in ALLOWED_RESOURCES:
        return f'Invalid type - valid type is: {",".join(ALLOWED_RESOURCES)} - requested {type}'
    return ''


def request_validation(job: str, run_type: str, priority: int):
    if not job or not run_type:
        return 'Invalid request - both job and run_type are required'
    if len(job) > 255:
        return f'Invalid job - max allowed length is 255 chars - requested {len(job)}'
    if run_type.lower() not in ALLOWED_RESOURCES and run_type.lower() != 'any':
        return f'Invalid run_type - valid run_type is: {",".join(ALLOWED_RESOURCES)} - requested {run_type}'
    if 5 < priority < 1:
        return f'Invalid priority - valid priority is between 1-5 (5-urgent) - requested {priority}'
    return ''


