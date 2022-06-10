from django.shortcuts import render, redirect
from django.http import HttpResponse
from login.models import UserInfo

# Create your views here.
def login(request):
    if request.method == "GET":
        return render(request, 'login.html')
    elif request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")

        # check username can be found in authdb
        try:
            check_user = UserInfo.objects.get(username=username)
        except:
            err_log = "User doesn\t exist. Please sign up first."
            return render(request, 'login.html', {'err_log': err_log})

        # get the user password from authdb
        check_password = check_user.password

        # check if the password is correct
        if check_password == password:
            return redirect('/userlist/')
        else:
            err_log = "Wrong username or password"
            return render(request, 'login.html', {'err_log': err_log})

def signup(request):
    if request.method == "GET":
        return render(request, 'signup.html')
    elif request.method == "POST":
        firstname = request.POST.get("firstname")
        lastname = request.POST.get("lastname")
        username = request.POST.get("username")
        email = request.POST.get("email")
        password = request.POST.get("password")

        UserInfo.objects.create(firstname=firstname, lastname=lastname, username=username, email=email, password=password)

        return redirect('/login/')

def userlist(request):
    user_list = UserInfo.objects.all()
    return render(request, 'userlist.html', {'user_list': user_list})

def delete(request):
    try:
        get_id = request.GET.get('get_id')
        UserInfo.objects.get(id=get_id).delete()
        return redirect('/userlist/')
    except:
        return HttpResponse('The input id does not exist.')