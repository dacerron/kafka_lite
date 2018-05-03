var Producer = {}

Producer = (function () {

    var events = function () {
        $(function() {
            $("#message-submit").on("click", function () {
                start(0)
            })
        }, false)
    }

    var start = function(counter) {
        setTimeout(function() {
            let ip = $("#ip").val()
            let pid = $("#partition").val()
            console.log("ip is" + ip)
            hideSadDog()
            bork()
            sendMessage($("#message").val() + " " + counter, pid,  ip).then(function (result) {
                console.log("result: " +result)
                doggo()
                start(counter + 1)
            }).catch(function (error) {
                console.log(error)
                showSadDog()
                doggo()
            })
        }, 1000);

    }

    var sendMessage = function (msg, pid, ip) {
        console.log("sending:" + msg)
        return fetch("http://" + ip + "/produce?message=" + msg + "&pid=" + pid, {
            mode: 'no-cors'
        })
        /*
        return new Promise(function (resolve, reject) {
            var xdom = new XMLHttpRequest()
            xdom.onreadystatechange = function () {
                if (this.readyState === 4 && this.status === 200) {
                    resolve()
                } else {
                    reject()
                }
            }

            xdom.open("POST", "127.0.0.1:8080/produce")
            xdom.setRequestHeader("Content-type", "application/x-www-form-urlencoded")
            xdom.send("message=" + msg)
        })
        */
    }

    var bork = function () {
        document.querySelector("#dog1").classList.add("hidden")
        document.querySelector("#dog2").classList.remove("hidden")
    }

    var doggo = function () {
        document.querySelector("#dog1").classList.remove("hidden")
        document.querySelector("#dog2").classList.add("hidden")
    }

    var hideSadDog = function () {
        document.querySelector("#sadDog").classList.add("hidden")
    }

    var showSadDog = function () {
        document.querySelector("#sadDog").classList.remove("hidden")
    }

    return {
        init: events
    }
})()

Producer.init()
