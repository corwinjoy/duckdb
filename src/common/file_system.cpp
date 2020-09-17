#include "duckdb/common/file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {
using namespace std;

#include <cstdio>

FileSystem &FileSystem::GetFileSystem(ClientContext &context) {
	return *context.db.config.file_system;
}

static void AssertValidFileFlags(uint8_t flags) {
	// cannot combine Read and Write flags
	assert(!(flags & FileFlags::FILE_FLAGS_READ && flags & FileFlags::FILE_FLAGS_WRITE));
	// cannot combine Read and CREATE/Append flags
	assert(!(flags & FileFlags::FILE_FLAGS_READ && flags & FileFlags::FILE_FLAGS_APPEND));
	assert(!(flags & FileFlags::FILE_FLAGS_READ && flags & FileFlags::FILE_FLAGS_FILE_CREATE));
	assert(!(flags & FileFlags::FILE_FLAGS_READ && flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
	// cannot combine CREATE and CREATE_NEW flags
	assert(!(flags & FileFlags::FILE_FLAGS_FILE_CREATE && flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
}

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// somehow sometimes this is missing
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

// Solaris
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

struct UnixFileHandle : public FileHandle {
public:
	UnixFileHandle(FileSystem &file_system, string path, int fd) : FileHandle(file_system, path), fd(fd) {
	}
	virtual ~UnixFileHandle() {
		Close();
	}

protected:
	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};

public:
	int fd;
};

unique_ptr<FileHandle> FileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock_type) {
	AssertValidFileFlags(flags);

	int open_flags = 0;
	int rc;
	if (flags & FileFlags::FILE_FLAGS_READ) {
		open_flags = O_RDONLY;
	} else {
		// need Read or Write
		assert(flags & FileFlags::FILE_FLAGS_WRITE);
		open_flags = O_RDWR | O_CLOEXEC;
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			open_flags |= O_CREAT;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			open_flags |= O_CREAT | O_TRUNC;
		}
		if (flags & FileFlags::FILE_FLAGS_APPEND) {
			open_flags |= O_APPEND;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
#if defined(__sun) && defined(__SVR4)
		throw Exception("DIRECT_IO not supported on Solaris");
#endif
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
		// OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
		open_flags |= O_SYNC;
#else
		open_flags |= O_DIRECT | O_SYNC;
#endif
	}
	int fd = open(path, open_flags, 0666);
	if (fd == -1) {
		throw IOException("Cannot open file \"%s\": %s", path, strerror(errno));
	}
	// #if defined(__DARWIN__) || defined(__APPLE__)
	// 	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
	// 		// OSX requires fcntl for Direct IO
	// 		rc = fcntl(fd, F_NOCACHE, 1);
	// 		if (fd == -1) {
	// 			throw IOException("Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
	// 		}
	// 	}
	// #endif
	if (lock_type != FileLockType::NO_LOCK) {
		// set lock on file
		struct flock fl;
		memset(&fl, 0, sizeof fl);
		fl.l_type = lock_type == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
		fl.l_whence = SEEK_SET;
		fl.l_start = 0;
		fl.l_len = 0;
		rc = fcntl(fd, F_SETLK, &fl);
		if (rc == -1) {
			throw IOException("Could not set lock on file \"%s\": %s", path, strerror(errno));
		}
	}
	return make_unique<UnixFileHandle>(*this, path, fd);
}

void FileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t offset = lseek(fd, location, SEEK_SET);
	if (offset == (off_t)-1) {
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path,
		                  strerror(errno));
	}
}

int64_t FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_read = read(fd, buffer, nr_bytes);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_read;
}

int64_t FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_written = write(fd, buffer, nr_bytes);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_written;
}

int64_t FileSystem::GetFileSize(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return -1;
	}
	return s.st_size;
}

void FileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (ftruncate(fd, new_size) != 0) {
		throw IOException("Could not truncate file \"%s\": %s", handle.path, strerror(errno));
	}
}

bool FileSystem::DirectoryExists(const string &directory) {
	if (!directory.empty()) {
		if (access(directory.c_str(), 0) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR)
				return true;
		}
	}
	// if any condition fails
	return false;
}

bool FileSystem::FileExists(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (!(status.st_mode & S_IFDIR))
				return true;
		}
	}
	// if any condition fails
	return false;
}

void FileSystem::CreateDirectory(const string &directory) {
	struct stat st;

	if (stat(directory.c_str(), &st) != 0) {
		/* Directory does not exist. EEXIST for race condition */
		if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
			throw IOException("Failed to create directory \"%s\"!", directory);
		}
	} else if (!S_ISDIR(st.st_mode)) {
		throw IOException("Failed to create directory \"%s\": path exists but is not a directory!", directory);
	}
}

int remove_directory_recursively(const char *path) {
	DIR *d = opendir(path);
	idx_t path_len = (idx_t)strlen(path);
	int r = -1;

	if (d) {
		struct dirent *p;
		r = 0;
		while (!r && (p = readdir(d))) {
			int r2 = -1;
			char *buf;
			idx_t len;
			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}
			len = path_len + (idx_t)strlen(p->d_name) + 2;
			buf = new char[len];
			if (buf) {
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path, p->d_name);
				if (!stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = remove_directory_recursively(buf);
					} else {
						r2 = unlink(buf);
					}
				}
				delete[] buf;
			}
			r = r2;
		}
		closedir(d);
	}
	if (!r) {
		r = rmdir(path);
	}
	return r;
}

void FileSystem::RemoveDirectory(const string &directory) {
	remove_directory_recursively(directory.c_str());
}

void FileSystem::RemoveFile(const string &filename) {
	if (std::remove(filename.c_str()) != 0) {
		throw IOException("Could not remove file \"%s\": %s", filename, strerror(errno));
	}
}

bool FileSystem::ListFiles(const string &directory, function<void(string, bool)> callback) {
	if (!DirectoryExists(directory)) {
		return false;
	}
	DIR *dir = opendir(directory.c_str());
	if (!dir) {
		return false;
	}
	struct dirent *ent;
	// loop over all files in the directory
	while ((ent = readdir(dir)) != NULL) {
		string name = string(ent->d_name);
		// skip . .. and empty files
		if (name.empty() || name == "." || name == "..") {
			continue;
		}
		// now stat the file to figure out if it is a regular file or directory
		string full_path = JoinPath(directory, name);
		if (access(full_path.c_str(), 0) != 0) {
			continue;
		}
		struct stat status;
		stat(full_path.c_str(), &status);
		if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
			// not a file or directory: skip
			continue;
		}
		// invoke callback
		callback(name, status.st_mode & S_IFDIR);
	}
	closedir(dir);
	return true;
}

string FileSystem::PathSeparator() {
	return "/";
}

void FileSystem::FileSync(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (fsync(fd) != 0) {
		throw FatalException("fsync failed!");
	}
}

void FileSystem::MoveFile(const string &source, const string &target) {
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(source.c_str(), target.c_str()) != 0) {
		throw IOException("Could not rename file!");
	}
}

void FileSystem::SetWorkingDirectory(string path) {
	if (chdir(path.c_str()) != 0) {
		throw IOException("Could not change working directory!");
	}
}

#else

#include <string>
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#undef FILE_CREATE // woo mingw

// Returns the last Win32 error, in string format. Returns an empty string if there is no error.
std::string GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = GetLastError();
	if (errorMessageID == 0)
		return std::string(); // No error message has been recorded

	LPSTR messageBuffer = nullptr;
	idx_t size =
	    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	                   NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

	std::string message(messageBuffer, size);

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}

struct WindowsFileHandle : public FileHandle {
public:
	WindowsFileHandle(FileSystem &file_system, string path, HANDLE fd) : FileHandle(file_system, path), fd(fd) {
	}
	virtual ~WindowsFileHandle() {
		Close();
	}

protected:
	void Close() override {
		CloseHandle(fd);
	};

public:
	HANDLE fd;
};

unique_ptr<FileHandle> FileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock_type) {
	AssertValidFileFlags(flags);

	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;
	DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
	if (flags & FileFlags::FILE_FLAGS_READ) {
		desired_access = GENERIC_READ;
		share_mode = FILE_SHARE_READ;
	} else {
		// need Read or Write
		assert(flags & FileFlags::FILE_FLAGS_WRITE);
		desired_access = GENERIC_READ | GENERIC_WRITE;
		share_mode = 0;
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			creation_disposition = OPEN_ALWAYS;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			creation_disposition = CREATE_ALWAYS;
		}
		if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
			flags_and_attributes |= FILE_FLAG_WRITE_THROUGH;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
		flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
	}
	HANDLE hFile =
	    CreateFileA(path, desired_access, share_mode, NULL, creation_disposition, flags_and_attributes, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		auto error = GetLastErrorAsString();
		throw IOException("Cannot open file \"%s\": %s", path, error);
	}
	auto handle = make_unique<WindowsFileHandle>(*this, path, hFile);
	if (flags & FileFlags::FILE_FLAGS_APPEND) {
		SetFilePointer(*handle, GetFileSize(*handle));
	}
	return move(handle);
}

void FileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	LARGE_INTEGER loc;
	loc.QuadPart = location;
	auto rc = SetFilePointerEx(hFile, loc, NULL, FILE_BEGIN);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path, error);
	}
}

int64_t FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	DWORD bytes_read;
	auto rc = ReadFile(hFile, buffer, (DWORD)nr_bytes, &bytes_read, NULL);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not write file \"%s\": %s", handle.path, error);
	}
	return bytes_read;
}

int64_t FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	DWORD bytes_read;
	auto rc = WriteFile(hFile, buffer, (DWORD)nr_bytes, &bytes_read, NULL);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not write file \"%s\": %s", handle.path, error);
	}
	return bytes_read;
}

int64_t FileSystem::GetFileSize(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	LARGE_INTEGER result;
	if (!GetFileSizeEx(hFile, &result)) {
		return -1;
	}
	return result.QuadPart;
}

void FileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	// seek to the location
	SetFilePointer(handle, new_size);
	// now set the end of file position
	if (!SetEndOfFile(hFile)) {
		auto error = GetLastErrorAsString();
		throw IOException("Failure in SetEndOfFile call on file \"%s\": %s", handle.path, error);
	}
}

bool FileSystem::DirectoryExists(const string &directory) {
	DWORD attrs = GetFileAttributesA(directory.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

bool FileSystem::FileExists(const string &filename) {
	DWORD attrs = GetFileAttributesA(filename.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void FileSystem::CreateDirectory(const string &directory) {
	if (DirectoryExists(directory)) {
		return;
	}
	if (directory.empty() || !CreateDirectoryA(directory.c_str(), NULL) || !DirectoryExists(directory)) {
		throw IOException("Could not create directory!");
	}
}

static void delete_dir_special_snowflake_windows(string directory) {
	if (directory.size() + 3 > MAX_PATH) {
		throw IOException("Pathname too long");
	}
	// create search pattern
	TCHAR szDir[MAX_PATH];
	snprintf(szDir, MAX_PATH, "%s\\*", directory.c_str());

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(szDir, &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return;
	}

	do {
		if (string(ffd.cFileName) == "." || string(ffd.cFileName) == "..") {
			continue;
		}
		if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			// recurse to zap directory contents
			FileSystem fs;
			delete_dir_special_snowflake_windows(fs.JoinPath(directory, ffd.cFileName));
		} else {
			if (strlen(ffd.cFileName) + directory.size() + 1 > MAX_PATH) {
				throw IOException("Pathname too long");
			}
			// create search pattern
			TCHAR del_path[MAX_PATH];
			snprintf(del_path, MAX_PATH, "%s\\%s", directory.c_str(), ffd.cFileName);
			if (!DeleteFileA(del_path)) {
				throw IOException("Failed to delete directory entry");
			}
		}
	} while (FindNextFile(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		throw IOException("Something went wrong");
	}
	FindClose(hFind);

	if (!RemoveDirectoryA(directory.c_str())) {
		throw IOException("Failed to delete directory");
	}
}

void FileSystem::RemoveDirectory(const string &directory) {
	delete_dir_special_snowflake_windows(directory.c_str());
}

void FileSystem::RemoveFile(const string &filename) {
	DeleteFileA(filename.c_str());
}

bool FileSystem::ListFiles(const string &directory, function<void(string, bool)> callback) {
	string search_dir = JoinPath(directory, "*");

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(search_dir.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
		string cFileName = string(ffd.cFileName);
		if (cFileName == "." || cFileName == "..") {
			continue;
		}
		callback(cFileName, ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
	} while (FindNextFile(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		FindClose(hFind);
		return false;
	}

	FindClose(hFind);
	return true;
}

string FileSystem::PathSeparator() {
	return "\\";
}

void FileSystem::FileSync(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	if (FlushFileBuffers(hFile) == 0) {
		throw IOException("Could not flush file handle to disk!");
	}
}

void FileSystem::MoveFile(const string &source, const string &target) {
	if (!MoveFileA(source.c_str(), target.c_str())) {
		throw IOException("Could not move file");
	}
}

void FileSystem::SetWorkingDirectory(string path) {
	if (!SetCurrentDirectory(path.c_str())) {
		throw IOException("Could not change working directory!");
	}
}
#endif

void FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// seek to the location
	SetFilePointer(handle, location);
	// now read from the location
	int64_t bytes_read = Read(handle, buffer, nr_bytes);
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read sufficient bytes from file \"%s\"", handle.path);
	}
}

void FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// seek to the location
	SetFilePointer(handle, location);
	// now write to the location
	int64_t bytes_written = Write(handle, buffer, nr_bytes);
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write sufficient bytes from file \"%s\"", handle.path);
	}
}

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}

void FileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

void FileHandle::Sync() {
	file_system.FileSync(*this);
}

void FileHandle::Truncate(int64_t new_size) {
	file_system.Truncate(*this, new_size);
}

static bool HasGlob(const string &str) {
	for(idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '*' || str[i] == '?') {
			return true;
		}
	}
	return false;
}


static void GlobFiles(FileSystem &fs, const string &path, const string &glob, bool match_directory, vector<string> &result) {
	fs.ListFiles(path, [&](const string &fname, bool is_directory) {
		if (is_directory != match_directory) {
			return;
		}
		if (LikeFun::Glob(fname.c_str(), glob.c_str(), "\\")) {
			result.push_back(fs.JoinPath(path, fname));
		}
	});
}

vector<string> FileSystem::Glob(string path) {
	// first check if the path has a glob at all
	if (!HasGlob(path)) {
		// no glob: return only the file (if it exists)
		vector<string> result;
		if (FileExists(path)) {
			result.push_back(path);
		}
		return result;
	}
	// split up the path into separate chunks
	vector<string> splits;
	idx_t last_pos = 0;
	for(idx_t i = 0; i < path.size(); i++) {
		if (path[i] == '\\' || path[i] == '/') {
			splits.push_back(path.substr(last_pos, i - last_pos));
			last_pos = i + 1;
		}
	}
	splits.push_back(path.substr(last_pos, path.size() - last_pos));
	// now iterate over the chunks
	vector<string> previous_directories;
	for(idx_t i = 0; i < splits.size(); i++) {
		bool is_last_chunk = i + 1 == splits.size();
		// if it's the last chunk we need to find files, otherwise we find directories
		// not the last chunk: gather a list of all directories that match the glob pattern
		vector<string> result;
		if (previous_directories.empty()) {
			// no previous directories: list in the current path
			GlobFiles(*this, ".", splits[i], !is_last_chunk, result);
		} else {
			// previous directories
			// we iterate over each of the previous directories, and apply the glob of the current directory
			for(auto &prev_directory : previous_directories) {
				GlobFiles(*this, prev_directory, splits[i], !is_last_chunk, result);
			}
		}
		if (is_last_chunk || result.size() == 0) {
			return result;
		}
		previous_directories = move(result);
	}
	throw InternalException("Eeek");
}

} // namespace duckdb
