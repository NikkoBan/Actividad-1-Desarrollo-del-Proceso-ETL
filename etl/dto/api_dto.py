"""DTOs para datos provenientes de la API REST."""

from pydantic import BaseModel, Field


class UserDTO(BaseModel):
    id: int
    name: str
    username: str
    email: str
    phone: str | None = None
    website: str | None = None


class PostDTO(BaseModel):
    id: int
    user_id: int = Field(..., alias="userId")
    title: str
    body: str

    model_config = {"populate_by_name": True}


class CommentDTO(BaseModel):
    id: int
    post_id: int = Field(..., alias="postId")
    name: str
    email: str
    body: str

    model_config = {"populate_by_name": True}

