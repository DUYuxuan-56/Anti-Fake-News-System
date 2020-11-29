exception IllegalArgument {
  1: string message;
}

service BcryptService {
 void checkAndUpdateMap (1: string host, 2: string port) throws (1: IllegalArgument e);
 string signature(1: string content, 2: i32 seckeyNum);
}
