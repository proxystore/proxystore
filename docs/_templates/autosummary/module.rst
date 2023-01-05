{{ fullname | escape | underline}}

{% block modules %}
{% if modules %}
.. rubric:: Modules

.. autosummary::
   :toctree:
   :template: autosummary/module.rst
   :recursive:

{% for item in modules %}
   {{ item }}
{% endfor %}
{% endif %}
{% endblock %}

.. automodule:: {{ fullname }}
   :members:
   :inherited-members:
   :member-order: bysource
   :show-inheritance:
