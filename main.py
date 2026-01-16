import sqlite3

from fastapi import FastAPI
import requests
from typing import Any

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/sum")
def sum(x: int = 0, y: int = 10):
    return x + y


@app.get("/geocode")
def geocode(lat: float, lon: float):
    url = f"https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={lat}&lon={lon}"

    request = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

    return request.json()


@app.get('/movies')
def get_movies():
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()
    cursor.execute('SELECT * FROM movie')

    output = []
    for movie in cursor:
        m = {'id: ': movie[0], 'title: ': movie[1], 'director: ': movie[2], 'year: ': movie[3],
             'description: ': movie[4], }
        output.append(m)
    return output


@app.get('/movies/{movie_id}')
def get_single_movie(movie_id: int):
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()
    movie = cursor.execute('SELECT * FROM movie WHERE id=?', (movie_id,)).fetchone()
    if movie is None:
        return {"message": 'Movie not found'}
    return {'id: ': movie[0], 'title: ': movie[1], 'year: ': movie[2], 'director: ': movie[3],
            'description: ': movie[4], }


@app.post('/movies')
def add_movie(params: dict[str, Any]):
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()

    title = params.get('title')
    year = params.get('year')
    director = params.get('director')
    description = params.get('description')

    if not title or not year or not director or not description:
        return {'message': 'Missing required fields'}

    cursor.execute(
        "INSERT INTO movie (title, year, director, description) VALUES (?,?,?,?)",
        (title, year, director, description)
    )

    db.commit()

    new_id = cursor.lastrowid
    db.close()

    return {
        "message": "Movie added successfully",
        "id": new_id
    }


@app.delete('/movies/{movie_id}')
def delete_movie(movie_id: int):
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()

    movie = cursor.execute(
        "SELECT * FROM movie WHERE id = ?",
        (movie_id,)
    ).fetchone()

    if movie is None:
        db.close()
        return {"message": "Movie not found."}

    cursor.execute(
        "DELETE FROM movie WHERE id = ?",
        (movie_id,)
    )
    db.commit()
    db.close()

    return {"message": f"Movie {movie_id} deleted successfully"}


@app.put('/movies/{movie_id}')
def update_movie(movie_id: int, params: dict[str, Any]):
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()

    movie = cursor.execute(
        "SELECT * FROM movie WHERE id = ?",
        (movie_id,)
    ).fetchone()

    if movie is None:
        db.close()
        return {"message": "Movie not found."}

    title = params.get("title", movie[1])
    director = params.get("director", movie[2])
    year = params.get("year", movie[3])
    description = params.get("description", movie[4])

    cursor.execute(
        "UPDATE movie SET title = ?, year = ?, director = ?, description = ? WHERE id = ?",
        (title, year, director, description, movie_id)
    )
    db.commit()
    db.close()

    return {"message": f"Movie {movie_id} updated successfully"}


@app.delete('/movies')
def delete_all_movies():
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()
    cursor.execute("DELETE FROM movie")
    db.commit()
    db.close()

    return {"message": f"{cursor.rowcount} movies have been deleted"}


@app.get('/actors')
def get_actors():
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()
    cursor.execute('SELECT * FROM actor')

    output = []
    for actor in cursor:
        m = {'id: ': actor[0], 'name: ': actor[1], 'surname: ': actor[2], }
        output.append(m)
    return output


@app.get('/actors/{actor_id}')
def get_single_actor(actor_id: int):
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()
    actor = cursor.execute('SELECT * FROM actor WHERE id=?', (actor_id,)).fetchone()
    if actor is None:
        return {"message": 'Actor not found'}
    return {'id: ': actor[0], 'name: ': actor[1], 'surname: ': actor[2], }


@app.post('/actors')
def add_actor(params: dict[str, Any]):
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()

    name = params.get('name')
    surname = params.get('surname')

    if not name or not surname:
        return {'message': 'Missing required fields'}

    cursor.execute(
        "INSERT INTO actor (name, surname) VALUES (?,?)",
        (name, surname)
    )

    db.commit()
    db.close()

    return {
        "message": "Actor added successfully",
        "id": cursor.lastrowid
    }


@app.delete('/actors/{actor_id}')
def delete_actor(actor_id: int):
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()

    actor = cursor.execute(
        "SELECT * FROM actor WHERE id = ?",
        (actor_id,)
    ).fetchone()

    if actor is None:
        db.close()
        return {"message": "Actor not found."}

    cursor.execute(
        "DELETE FROM actor WHERE id = ?",
        (actor_id,)
    )
    db.commit()
    db.close()

    return {"message": f"Actor {actor_id} deleted successfully"}


@app.put('/actors/{actor_id}')
def update_actor(actor_id: int, params: dict[str, Any]):
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()

    actor = cursor.execute(
        "SELECT * FROM actor WHERE id = ?",
        (actor_id,)
    ).fetchone()

    if actor is None:
        db.close()
        return {"message": "Actor not found."}

    name = params.get("name", actor[1])
    surname = params.get("surname", actor[2])

    cursor.execute(
        "UPDATE actor SET name = ?, surname = ? WHERE id = ?",
        (name, surname, actor_id,)
    )
    db.commit()
    db.close()

    return {"message": f"Actor {actor_id} updated successfully"}


@app.delete('/actors')
def delete_all_actors():
    db = sqlite3.connect("movies-extended.db")
    cursor = db.cursor()
    cursor.execute("DELETE FROM actor")
    db.commit()
    db.close()

    return {"message": f"{cursor.rowcount} actor have been deleted"}


@app.get('/movies/{movie_id}/actors')
def get_actor_from_movie(movie_id: int):
    db = sqlite3.connect('movies-extended.db')
    cursor = db.cursor()
    cursor.execute(
        'SELECT actor.id, actor.name, actor.surname FROM actor '
        'LEFT JOIN movie_actor_through ON actor.id = movie_actor_through.actor_id WHERE movie_actor_through.movie_id=?',
        (movie_id,))

    output = []
    for actor in cursor:
        m = {'id: ': actor[0], 'name: ': actor[1], 'surname: ': actor[2], }
        output.append(m)

    db.close()

    if len(output) == 0:
        return {"message": 'Actor not found'}

    return output
