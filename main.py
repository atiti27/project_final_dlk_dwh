def run_script(file_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        script = file.read()
        exec(script, globals())


run_script('df_population.py')
run_script('df_qualite_air.py')
run_script('df_transports.py')
run_script('df_population_vehicule.py')

