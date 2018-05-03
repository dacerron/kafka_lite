var Consumer = {}
var msgs = []

Consumer = (function () {

    var events = function () {
        $(function() {
            $("#message-submit").on("click", function (e) {
                let ip = $("#ip").val()
                console.log("ip is" + ip)
                e.preventDefault()
                startPoll(ip);
            })
        }, false)
    }

    var startPoll = function(ip) {
        hideSadDog()
        bork()
        fetch("http://" + ip + "/consume?message=poll").then((resp) => resp.json()).then(function (data) {
            // console.log("result: " + data)
            // $("#msg-table").find("tr:gt(0)").remove()
            $.each(data, function(i, item) {
                var $tr = $('<tr>').append(
                    $('<td>').text(item.PartitionId),
                    $('<td>').text(item.Offset),
                    $('<td>').text(atob(item.Value)),
                ).appendTo('#msg-table');
                // console.log($tr.wrap('<p>').html());
            });
            setTimeout(function(){doggo()}, 1000)
        }).catch(function (error) {
            console.log(error)
            showSadDog()
            setTimeout(function(){doggo()}, 1000)
        })
        setTimeout(function(){startPoll(ip)}, 2000)
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

Consumer.init()
