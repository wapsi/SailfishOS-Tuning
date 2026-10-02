#include <unistd.h>

int main(int argc, char **argv)
{
    setuid(0);
    setgid(0);
    execv("/usr/local/sbin/sailfish-contact-lookup", argv);
    return 1;
}
