#!/usr/bin/env python
# coding: utf-8

# In[1]:


print('Iniciando processamento dos spark jobs')


# In[7]:


import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

def run_notebook(notebook_name):
    notebook_path = '/path/to/app/notebooks/' + notebook_name
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        ep.preprocess(nb, {'metadata': {'path': './'}})
        with open(notebook_path, 'w') as f:
            nbformat.write(nb, f)


# # Gera as filas de integração

# In[8]:


run_notebook('update_clientes_in_app.ipynb')


# In[10]:


run_notebook('insert_clientes_in_app.ipynb')


# In[12]:


print('-' * 30)
print('Executando update_contatos_in_app')
print('-' * 30)
run_notebook('update_contatos_in_app.ipynb')
