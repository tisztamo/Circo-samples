using Circo, CSV, DataFrames, Dates, HTTP, JSON, Test

struct Purchase
    amount::Int64
    followingsessions::Set
    #date::Date
    Purchase(amount::Int64) = new(amount, Set())
end

function sessioncount(purchases::Array{Purchase})::Int64
    allsessions = Set()
    for purchase in purchases
        union!(allsessions, purchase.followingsessions)
    end
    length(allsessions)
end

function testclass_stats(windowsize)
    purchases = Array{Purchase}(undef, 0)
    return function(event)
        if event.event == "purchase"
            count = sessioncount(purchases)
            push!(purchases, Purchase(event.amount))
            length(purchases) > windowsize && popfirst!(purchases)
            return (purchases=length(purchases), visitors=count)
        else
            length(purchases) > 0 &&
                push!(purchases[end].followingsessions, event.session_id)
        end
        (purchases=length(purchases), visitors=nothing)
    end
end

function windowed_abresults(windowsize = 100)
    classes = [testclass_stats(windowsize) for i=1:2]
    results = [(purchases=0, visitors=0), (purchases=0, visitors=0)]
    return (event) -> begin
        test_class = event.test_class + 1
        result = classes[test_class](event)
        if result[2] != nothing
            results[test_class] = result
            return results
        end
        nothing
    end
end

function abstatscalculator(;return_rawconfidence=false)
    return function(results)
        if results == nothing
            return nothing
        end
        (a_purchases, a_visitors) = results[1]
        (b_purchases, b_visitors) = results[2]
        if a_visitors == 0 || b_visitors == 0
            return nothing
        end
        url = "http://127.0.0.1:4000/abtestcalculator.php?control_visitors=$a_visitors&control_conversions=$a_purchases&treatment_visitors=$b_visitors&treatment_conversions=$b_purchases"
        r = HTTP.get(url)
        r.status == 200 || error("Got response from $url:\n$r")
        rbody = JSON.parse(String(r.body))
        rawconfidence = rbody["confidence"]
        if return_rawconfidence
            return rawconfidence
        end
        zScore = rbody["zScore"]
        confidence = zScore >= 0.0 ? rawconfidence : 1.0 - rawconfidence
        confidence
    end
end

function abstats_workflow(windowsize, outfilename)
    return windowed_abresults(windowsize) |
            abstatscalculator(return_rawconfidence=true) >
            outfilename
end

function switch_purchaseclass(source_test_class, dropped_ratio)
    dropped = 1
    return function(event)
        if event.test_class == source_test_class &&
            event.event == "purchase" &&
            rand() <= dropped_ratio
            dropped += 1
            return (test_class = 1 - event.test_class, session_id = event.session_id, event = event.event, qty = event.qty, amount = event.amount)
        end
        return event
    end
end

ispath("results") || mkdir("results")

windowsizes = [400]
for windowsize in windowsizes
    println("Processing window size $windowsize")

    outfilename = "results/aa_windowed_confidence_$windowsize.csv"
    rm(outfilename;force=true)
    aa_workflow = CSV.read("assets/test_0_events.csv") | abstats_workflow(windowsize, outfilename)
    @time result = aa_workflow()

    outfilename = "results/ab_windowed_confidence_$windowsize.csv"
    rm(outfilename;force=true)
    ab_workflow = CSV.read("assets/test_0_events.csv") |
                    switch_purchaseclass(1, 0.05) |
                    abstats_workflow(windowsize, outfilename)
    @time result = ab_workflow()

end

#@test result == 200
