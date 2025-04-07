FROM ghcr.io/astral-sh/uv:debian


# Set working directory
WORKDIR /app

# Copy your project files
COPY assets ./
COPY data.pq ./
COPY *.py ./
COPY pyproject.toml ./
COPY uv.lock ./

# # Install dependencies (uv creates a virtual environment automatically)
# RUN uv venv && \
#   uv pip install gunicorn && \
#   uv pip install .
RUN uv sync

# Set environment variables (optional)
# ENV PYTHONUNBUFFERED=1
ENV PORT=8050

# Expose the port Dash will run on
EXPOSE ${PORT}

# Run the Dash app using Gunicorn
CMD ["uv", "run", "gunicorn", "-b", "0.0.0.0:8050", "main:server"]
