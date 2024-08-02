import os
import uuid

import dotenv
from fastastra.fastastra import AstraDatabase
from fastcore.basics import patch
from starlette.responses import FileResponse

from fasthtml import Style, user_pwd_auth, FastHTML, picolink, AX, Li, Input, Div, Form, Group, Button, Card, Ul, \
    Titled, Hidden, Checkbox, fill_form
from fasthtml.fastapp import serve

dotenv.load_dotenv("./.env")
dotenv.load_dotenv("../.env")


token = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
dbid = os.environ["DBID"]


db = AstraDatabase(token, dbid)
todos = db.t.todos
if todos not in db.t: todos.create(id=uuid.uuid4, title=str, done=bool, pk='id')
Todo = todos.dataclass()

id_curr = 'current-todo'
def tid(id): return f'todo-{id}'

css = Style(':root { --pico-font-size: 100%; }')
auth = user_pwd_auth(user='s3kret', skip=[r'/favicon\.ico', r'/static/.*', r'.*\.css'])
app = FastHTML(hdrs=(picolink, css), middleware=[auth])
rt = app.route

@rt("/{fname:path}.{ext:static}")
async def get(fname:str, ext:str): return FileResponse(f'{fname}.{ext}')

@patch
def __ft__(self:Todo):
    show = AX(self.title, f'/todos/{self.id}', id_curr)
    edit = AX('edit',     f'/edit/{self.id}' , id_curr)
    dt = ' (done)' if self.done else ''
    return Li(show, dt, ' | ', edit, id=tid(self.id))

def mk_input(**kw): return Input(id="new-title", name="title", placeholder="New Todo", **kw)
def clr_details(): return Div(hx_swap_oob='innerHTML', id=id_curr)

@rt("/")
async def get(request):
    add = Form(Group(mk_input(), Button("Add")),
               hx_post="/", target_id='todo-list', hx_swap="beforeend")
    card = Card(Ul(*todos(), id='todo-list'),
                header=add, footer=Div(id=id_curr)),
    return Titled('Todo list', card)

@rt("/todos/{id}")
async def delete(id:str):
    todos.delete(id)
    return clr_details()

@rt("/")
async def post(todo:Todo):
    return todos.insert(todo), mk_input(hx_swap_oob='true')

@rt("/edit/{id}")
async def get(id:str):
    res = Form(Group(Input(id="title"), Button("Save")),
        Hidden(id="id"), Checkbox(id="done", label='Done'),
        hx_put="/", target_id=tid(id), id="edit")
    this_todo = todos[id]
    return fill_form(res, this_todo)

@rt("/")
async def put(todo: Todo): return todos.update(todo), clr_details()

@rt("/todos/{id}")
async def get(id:str):
    todo = todos[id]
    btn = Button('delete', hx_delete=f'/todos/{todo.id}',
                 target_id=tid(todo.id), hx_swap="outerHTML")
    return Div(Div(todo.title), btn)

serve()

