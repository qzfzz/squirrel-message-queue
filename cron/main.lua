-- The main function

function __main__()
	while true do
		local ok, item = item_pop_head()
		if ok then
			print(item)
			sleep(0, 100)
		end
	end
end
