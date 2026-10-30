import json

from django.http import HttpResponse
from django.urls import path


def client(request):
    return HttpResponse(json.dumps({
        "remote_addr": request.META.get("REMOTE_ADDR"),
        "secure": request.is_secure(),
        "url": request.build_absolute_uri("/"),
    }), content_type="application/json")


def logged_request_id(request):
    from ske_bridge.logging import request_id

    return HttpResponse(request_id.get() or "")


urlpatterns = [
    path("request-id", logged_request_id),
    path("", lambda request: HttpResponse("home")),
    path("client", client),
]
