import pyopa
log_pam1_env = pyopa.read_env_json(os.path.join(pyopa.matrix_dir(), 'logPAM1.json'))
s1 = pyopa.Sequence('HI')
s2 = pyopa.Sequence('GCANPSTLETNSQLVNRELIAVKINPRVYKGPNLGAFRL')

# super fast check whether the alignment reaches a given min-score
min_score = 100
pam250_env = pyopa.generate_env(log_pam1_env, 250, min_score)
pyopa.align_short(s1, s2, pam250_env)