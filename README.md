# MGNREGA Dashboard

This project is a web application built using Django for the backend and Bootstrap for responsive design. It allows citizens to select their district and view the performance of their district in the Mahatma Gandhi National Rural Employment Guarantee Act (MGNREGA) program.

## Features

- **District Selection**: Users can select their district from a list.
- **Performance Metrics**: View detailed performance metrics for the selected district.
- **Responsive Design**: The application is built with Bootstrap to ensure a seamless experience across devices.

## Project Structure

```
mgnrega-dashboard
├── manage.py
├── requirements.txt
├── mgnrega_dashboard
│   ├── __init__.py
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
├── apps
│   ├── __init__.py
│   ├── districts
│   │   ├── __init__.py
│   │   ├── admin.py
│   │   ├── apps.py
│   │   ├── models.py
│   │   ├── views.py
│   │   ├── urls.py
│   │   └── migrations
│   │       └── __init__.py
│   └── performance
│       ├── __init__.py
│       ├── admin.py
│       ├── apps.py
│       ├── models.py
│       ├── views.py
│       ├── urls.py
│       └── migrations
│           └── __init__.py
├── templates
│   ├── base.html
│   ├── districts
│   │   ├── district_list.html
│   │   └── district_detail.html
│   └── performance
│       └── performance_dashboard.html
├── static
│   ├── css
│   │   └── custom.css
│   ├── js
│   │   └── main.js
│   └── bootstrap
│       ├── css
│       └── js
├── media
├── config
│   ├── nginx.conf
│   ├── gunicorn.conf.py
│   └── supervisor.conf
└── README.md
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd mgnrega-dashboard
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Apply migrations:
   ```
   python manage.py migrate
   ```

5. Run the development server:
   ```
   python manage.py runserver
   ```

## Usage

- Navigate to `http://127.0.0.1:8000/` in your web browser.
- Select your district from the list to view its performance metrics.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.