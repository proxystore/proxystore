# Example ProxyStore Apps

Configure a new environment to try out the example apps.
```
$ python -m venv venv  # or $ virtualenv venv
$ . venv/bin/activate
$ pip install -r examples/requirements.txt
```

Notes:
- Most of the examples use Redis and require `redis-server` to
  be installed. See https://redis.io/docs/getting-started/ for more details.
- The examples should work with newer versions of the specified packages,
  but the `requirements.txt` represents the latest versions of the packages
  that the examples have been validated to work with.
