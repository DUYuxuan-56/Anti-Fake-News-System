service KeyValueService {
  void setPrimary(1: bool isPrimary);
  string collectiveSigning(1: string content);
}
