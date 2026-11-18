What happens to a file if the process crashes halfway through writing it?
-the files are stored in volume which is persistent storage so file is not lost

If the controller sends the same chunk twice, what should the worker do?
-when receving a chunk, if the worker found out if the chunk is already present then it should send a reply stating that it has already received the chunk, then the controller might try another worker. it also needs to check if the chunk's replica is 2

How would the worker know a chunk got corrupted on disk?
by checking the checksum number 

What should GET return if the stored bytes don’t match the checksum?
it should return an error and saying it got corrupted by some error code or any other means